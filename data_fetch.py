from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import os, zipfile
import random


def zip_folder(folder_to_zip, zip_file_name):
    # Create a zip file
    with zipfile.ZipFile(zip_file_name, "w", zipfile.ZIP_DEFLATED) as zipf:
        # Walk through the folder and add each file to the zip
        for root, _, files in os.walk(folder_to_zip):
            for file in files:
                file_path = os.path.join(root, file)
                # Write the file to the zip with its relative path
                zipf.write(file_path, os.path.relpath(file_path, folder_to_zip))


default_args = {
    "owner": "your_name",
    "start_date": datetime(2023, 9, 29),
    "retries": 1,
}


dag = DAG("data_fetch_pipeline", default_args=default_args, schedule_interval=None)

base_url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2023/"


# Define tasks
def archive_folder():
    zip_folder("/home/ganesh/bdl/a02/data", "/home/ganesh/bdl/a02/data_archived.zip")


def choose_random_files():
    number_of_files = 5
    links = []
    with open("/home/ganesh/bdl/a02/links.txt", "r") as file:
        for link in file:
            links.append(link)
    random_links = random.sample(links, number_of_files)
    with open("/home/ganesh/bdl/a02/random_links.txt", "w") as file:
        for link in random_links:
            file.write(base_url + link)


# Gets the link from the website and puts them in a "links.txt" file"
get_links_cmd = "curl -s https://www.ncei.noaa.gov/data/local-climatological-data/access/2023/ | grep -o -E 'href=\"([^\"]+)\"' | awk -F'\"' '{print $2}' > /home/ganesh/bdl/a02/links.txt"
fetch_files_cmd = r"cd /home/ganesh/bdl/a02/data && xargs -n 1 curl -O -J < /home/ganesh/bdl/a02/random_links.txt --create-dirs -P ./data"
get_links = BashOperator(
    task_id="get_links",
    bash_command=get_links_cmd,
    dag=dag,
)
choose_random = PythonOperator(
    task_id="choose_random_files",
    python_callable=choose_random_files,
    dag=dag,
)
fetch_files = BashOperator(task_id="fetch_files", bash_command=fetch_files_cmd, dag=dag)
archive_data = PythonOperator(
    task_id="archive_data", python_callable=archive_folder, dag=dag
)
# Set task dependencies
get_links >> choose_random >> fetch_files >> archive_data
