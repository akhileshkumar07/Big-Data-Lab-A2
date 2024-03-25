from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from bs4 import BeautifulSoup
import random
import subprocess

#####################################################################################
default_args = {
    'owner': 'akhilesh',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# define a DAG
dag = DAG(
    'noaa_data_fetch_pipeline',
    default_args=default_args,
    description='Pipeline to fetch and process NOAA data using Apache Airflow',
    schedule_interval=None,
)

#####################################################################################
# some parameters
BASE_URL = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/'
YEAR = '2023'
OUTPUT_PATH = '/home/akhilesh/big_data_lab/'

#####################################################################################
# Task 1: Fetch the webpage of the datasets
fetch_page_task = BashOperator(
    task_id='fetch_page',
    bash_command=f'wget -O /home/akhilesh/page_fetch.html {BASE_URL}{YEAR}/',
    dag=dag,
)

#####################################################################################
# Task 2: Extract CSV file links
def extract_csv_links():
    with open('/home/akhilesh/page_fetch.html', 'r', encoding='utf-8') as file:
        html_content = file.read()
        soup = BeautifulSoup(html_content, 'html.parser')
        csv_links = []
        for link in soup.find_all('a'):
            href = link.get('href')
            if href.endswith('.csv'):
                csv_links.append(href)
    return csv_links
        

extract_links_task = PythonOperator(
    task_id='extract_links',
    python_callable=extract_csv_links,
    provide_context=True,
    dag=dag,
)

#####################################################################################
# Task 3: Select random data files from the available list of csv files
def select_random_files(**kwargs):
    csv_links = kwargs['ti'].xcom_pull(task_ids='extract_links')
    num_files_required = 3
    return random.sample(csv_links, num_files_required)

select_files_task = PythonOperator(
    task_id='select_files',
    python_callable=select_random_files,
    provide_context=True,
    dag=dag,
)

#####################################################################################
# Task 4: Fetch individual csv files
def fetch_data_files(**kwargs):
    selected_files = kwargs['ti'].xcom_pull(task_ids='select_files')
    for filename in selected_files:
            file_url = f"{BASE_URL}{YEAR}/{filename}"
            subprocess.run(["wget", "-q", "-o", "/dev/null", file_url, "-P", OUTPUT_PATH])

fetch_files_task = PythonOperator(
    task_id='fetch_files',
    python_callable=fetch_data_files,
    provide_context=True,
    dag=dag,
)

#####################################################################################
# Task 5: Zip the selected data files
def zip_files():    
    subprocess.run(["zip", "noaa_archive.zip", OUTPUT_PATH])

zip_files_task = PythonOperator(
    task_id='zip_files',
    python_callable=zip_files,
    provide_context=True,
    dag=dag,
)

#####################################################################################
# Task 6: Move the archive to a desired location
place_archive_task = BashOperator(
    task_id='place_archive',
    bash_command=f'mv /home/akhilesh/noaa_archive.zip {OUTPUT_PATH}',
    dag=dag,
)

#####################################################################################

fetch_page_task >> extract_links_task >> select_files_task >> fetch_files_task >> zip_files_task >> place_archive_task
