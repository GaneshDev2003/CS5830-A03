import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.sensors.filesystem import FileSensor
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.runners.interactive.interactive_beam as ib
import csv
from airflow.operators.python import PythonOperator
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point
from geodatasets import get_path
import numpy as np


# Define default arguments
default_args = {
    "owner": "your_name",
    "start_date": datetime(2023, 9, 29),
    "retries": 1,
}

unzip_dir = "/home/ganesh/bdl/a02/climate_data/"
output_dir = "/home/ganesh/bdl/a02/"
root_dir = "/home/ganesh/bdl/a02/"


def process_task():
    def process_csv(csv_file):
        csv_path = os.path.join(unzip_dir, csv_file)
        df = pd.read_csv(csv_path)
        # Extract required fields
        required_fields = [
            "HourlyWindSpeed",
            "HourlyDryBulbTemperature",
        ]  # Add more fields as needed
        # Drop rows with NaN values in the required fields
        df = df.dropna(subset=required_fields)
        # Filter the DataFrame based on the required fields
        filtered_df = df[["DATE"] + required_fields]
        # Extract Lat/Long values
        lat_lon = (df["LATITUDE"].iloc[0], df["LONGITUDE"].iloc[0])
        # Create a tuple of the form <Lat, Lon, [[Date, Windspeed, BulbTemperature, RelativeHumidity, WindDirection], ...]>
        return lat_lon, filtered_df.values.tolist()

    with beam.Pipeline(options=PipelineOptions()) as pipeline:
        # Read from CSV file
        pcollection = (
            pipeline
            | "Read csv files in folder" >> beam.Create(os.listdir(unzip_dir))
            | "Parse CSV" >> beam.Map(process_csv)
            | "Write output"
            >> beam.io.WriteToText(os.path.join(output_dir, "result.txt"))
        )


def read_text_files(file_pattern):
    with open(file_pattern, "r") as file:
        lines = file.readlines()
        return [line.strip() for line in lines]


def monthly_average_task():
    output_file_path = "/home/ganesh/bdl/a02/averages.txt"

    def compute_average(line):
        data_tuple = eval(
            line.strip()
        )  # Assuming the tuple format is preserved in the result.txt file
        lat_lon = data_tuple[0]
        data_list = data_tuple[1]

        monthly_averages = {}
        for data_point in data_list:
            date_str, windspeed, bulb_temp = data_point
            month = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m")

            if month not in monthly_averages:
                monthly_averages[month] = {
                    "AverageWindSpeed": [],
                    "AverageDryBulbTemperature": [],
                }

            # Append values for each field, ignoring NaN values
            if not pd.isna(windspeed):
                monthly_averages[month]["AverageWindSpeed"].append(windspeed)

            if not pd.isna(bulb_temp):
                if not isinstance(bulb_temp, int):
                    bulb_temp = (
                        bulb_temp[: len(bulb_temp) - 1]
                        if bulb_temp[-1] == "s"
                        else bulb_temp
                    )
                    monthly_averages[month]["AverageDryBulbTemperature"].append(
                        float(bulb_temp)
                    )

        for month in monthly_averages.keys():
            monthly_averages[month]["AverageWindSpeed"] = sum(
                monthly_averages[month]["AverageWindSpeed"]
            ) / len(monthly_averages[month]["AverageWindSpeed"])
            monthly_averages[month]["AverageDryBulbTemperature"] = sum(
                monthly_averages[month]["AverageDryBulbTemperature"]
            ) / len(monthly_averages[month]["AverageDryBulbTemperature"])

        # Write the monthly averages with Lat/Long
        return lat_lon, monthly_averages

    with beam.Pipeline() as pipeline:
        result = (
            pipeline
            | "Read text file"
            >> beam.Create(
                read_text_files("/home/ganesh/bdl/a02/result.txt-00000-of-00001")
            )
            | "Compute avrages" >> beam.Map(compute_average)
            | "Write to text file" >> beam.io.WriteToText(output_file_path)
        )


def plot_images_task():

    def plot_images(line):
        month, data = eval(line.strip())
        df = pd.DataFrame(data)
        gdf = gpd.GeoDataFrame(
            df, geometry=gpd.points_from_xy(df.Longitude, df.Latitude), crs="EPSG:4326"
        )
        world = gpd.read_file(get_path("naturalearth.land"))

        # Plot heatmaps for each field
        fields = ["AverageWindSpeed", "AverageDryBulbTemperature"]
        for field in fields:
            ax = world.plot(color="white", edgecolor="black")
            gdf.plot(ax=ax, column=field, cmap="viridis", legend=True)
            ax.set_title(f"{field} data for the month {month}")
            plt.savefig(f"{root_dir}images/{month}_{field}_heatmap.png")

    def format_averages_file():
        with open("/home/ganesh/bdl/a02/averages.txt-00000-of-00001", "r") as file:
            data = [eval(line.strip()) for line in file]

        # Create a dictionary for each month
        months = {}
        for lat_lon, month_data in data:
            for month, values in month_data.items():
                if month not in months:
                    months[month] = {
                        "Latitude": [],
                        "Longitude": [],
                        "AverageWindSpeed": [],
                        "AverageDryBulbTemperature": [],
                    }
                months[month]["Latitude"].append(lat_lon[0])
                months[month]["Longitude"].append(lat_lon[1])
                months[month]["AverageWindSpeed"].append(values["AverageWindSpeed"])
                months[month]["AverageDryBulbTemperature"].append(
                    values["AverageDryBulbTemperature"]
                )
        with open("/home/ganesh/bdl/a02/formatted_averages.txt", "w") as file:
            for month, data in months.items():
                file.write("(" + str((month, data)) + ")\n")

    format_averages_file()
    with beam.Pipeline() as pipeline:
        result = (
            pipeline
            | "Read text file"
            >> beam.Create(
                read_text_files("/home/ganesh/bdl/a02/formatted_averages.txt")
            )
            | "Plot images" >> beam.Map(plot_images)
        )

    # Convert the dictionary to a DataFrame for each month


dag = DAG("analytics_pipeline", default_args=default_args, schedule_interval=None)

sense_file = FileSensor(
    task_id="wait_for_file",
    filepath="/home/ganesh/bdl/a02/data_archived.zip",
    timeout=5,
    dag=dag,
)

unzip = BashOperator(
    task_id="unzip",
    bash_command="unzip -d /home/ganesh/bdl/a02/climate_data /home/ganesh/bdl/a02/data_archived.zip",
    dag=dag,
)

filter_data = PythonOperator(
    task_id="process_data_task",
    python_callable=process_task,
    provide_context=True,
    dag=dag,
)

compute_monthly_average = PythonOperator(
    task_id="compute_monthly_average",
    python_callable=monthly_average_task,
    provide_context=True,
    dag=dag,
)

plot_heatmaps = PythonOperator(
    task_id="plot_heatmaps",
    python_callable=plot_images_task,
    provide_context=True,
    dag=dag,
)

sense_file >> unzip >> filter_data >> compute_monthly_average >> plot_heatmaps
