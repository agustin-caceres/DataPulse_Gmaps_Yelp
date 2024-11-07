#Librerías
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
#from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from google.cloud import bigquery
import os

# Función
from Pipelines.functions.etl_api import extraer_reviews_google_places, cargar_a_bigquery

######################################################################################
# PARÁMETROS
######################################################################################

API_KEY = 'AIzaSyDWx13fysNA9bZfNwS1-0fSqbGj9sl2AzQ'
businesses = [
    {"name": "Restaurante Guerrero", "location": "34.03664,-118.258789", "radius": 300},
    {"name": "La Mina Restaurante", "location": "25.868873,-80.33368", "radius": 300},
    {"name": "Restaurante Los Hernandez", "location": "25.868873,-80.33368", "radius": 300},
    {"name": "Las Tunas Restaurante", "location": "40.78967,-74.008296", "radius": 300},
    {"name": "La Cocina Pizza Restaurante", "location": "40.842095,-73.924665", "radius": 300},
]
bucket_name   = 'datos-api-places'
output_file   = 'reviews_data.json'
dataset_name  = '1'
table_name    = 'google_api_reviews'
project_id    = 'neon-gist-439401-k8'
owner         = 'Julieta'
nameDAG_base  = 'dag_etl_google_places_to_bigquery'

default_args = {
    'owner': owner,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

#######################################################################################
# DEFINICIÓN DEL DAG
#######################################################################################

with DAG(
    'dag_etl_google_places_to_bigquery',
    default_args=default_args,
    description='Extrae datos de Google Places, los guarda en GCS y los carga a BigQuery',
    #schedule_interval=timedelta(days=1),
    #start_date=datetime(2023, 11, 5),
    schedule_interval=None,
    catchup=False,
) as dag:

    # Tarea de inicio
    inicio = DummyOperator(task_id='inicio')

    # Tarea 1: Extraer datos de la API de Google Places y guardarlos en GCS
    def extraer_y_guardar():
        extraer_reviews_google_places(API_KEY, businesses, bucket_name, output_file)

    extraer_tarea = PythonOperator(
        task_id='extraer_reviews_y_guardar_en_gcs',
        python_callable=extraer_y_guardar,
    )

    # Tarea 2: Cargar el archivo JSON desde GCS a BigQuery
    cargar_a_bigquery_task = PythonOperator(
        task_id='cargar_a_bigquery',
        python_callable=cargar_a_bigquery,
        op_kwargs={
            'bucket_name': bucket_name,
            'output_file': output_file,
            'dataset_name': dataset_name,
            'table_name': table_name,
        },
    )


    # Tarea de fin
    fin = DummyOperator(task_id='fin')

    # Definir el flujo de tareas
    inicio >> extraer_tarea >> cargar_a_bigquery_task >> fin






    #    cargar_a_bigquery = GCSToBigQueryOperator(
     #   task_id='cargar_reviews_a_bigquery',
      #  bucket=bucket_name,
       # source_objects=[output_file],
       # destination_project_dataset_table=f'{project_id}.{dataset_name}.{table_name}',
       # source_format='NEWLINE_DELIMITED_JSON',
       # write_disposition='WRITE_TRUNCATE',
       # create_disposition='CREATE_IF_NEEDED',
    #)