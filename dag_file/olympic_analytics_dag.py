'''
@author: Rohit Sharma
Date: 2023-12-30
'''

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
import requests
from google.cloud import storage 
from io import BytesIO

default_args = {
    'owner': 'Rohit Sharma',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

urls = [
    'https://raw.githubusercontent.com/rs301378/Olympic-Data-Analytics-GCP/master/data/Athletes.csv',
    'https://raw.githubusercontent.com/rs301378/Olympic-Data-Analytics-GCP/master/data/Coaches.csv', 
    'https://raw.githubusercontent.com/rs301378/Olympic-Data-Analytics-GCP/master/data/EntriesGender.csv',
    'https://raw.githubusercontent.com/rs301378/Olympic-Data-Analytics-GCP/master/data/Medals.csv',
    'https://raw.githubusercontent.com/rs301378/Olympic-Data-Analytics-GCP/master/data/Teams.csv'
]

# Define the function to download files
def download_files(urls):
    for url in urls:
        gcs_bucket_name = 'us-east1-test-env-olympic-ab93b90d-bucket'
        gcs_folder_path = 'dags/data/raw/'

        # Initialize Google Cloud Storage client
        client = storage.Client()

        # Download each file
        for url in urls:
            response = requests.get(url, allow_redirects=True)
            content = BytesIO(response.content)

            # Extracting the filename from the URL
            filename = url.split('/')[-1]

            # Construct the full GCS path
            gcs_object_path = f"{gcs_folder_path}{filename}"

            # Upload content to Google Cloud Storage
            bucket = client.bucket(gcs_bucket_name)
            blob = bucket.blob(gcs_object_path)
            blob.upload_from_file(content, content_type='text/csv')

            print(f"Uploaded: {filename} to gs://{gcs_bucket_name}/{gcs_object_path}")


dag = DAG(
    dag_id='Olympic_Data_Analytics_GCP',
    default_args=default_args,
    description='End to End Olympic Data Analytics Project on GCP',
    schedule_interval='30 9 * * *',
)

start_task = DummyOperator(
    task_id='start', 
    dag=dag
)

download_task = PythonOperator(
    task_id= "download_data",
    python_callable=download_files,
    op_kwargs={'urls': urls},
    dag=dag,
)

end_task = DummyOperator(
    task_id='end', 
    dag=dag
) 

start_task >> download_task  >> end_task
