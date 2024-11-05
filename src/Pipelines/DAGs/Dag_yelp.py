# Librerías
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
from google.cloud import bigquery
from functions.bigquery_utils import crear_tabla_temporal, cargar_archivo_gcs_a_bigquery

######################################################################################
# PARÁMETROS PARA DATOS DE YELP
######################################################################################

nameDAG_base       = 'ETL_Yelp_Checkin_to_BQ'
project_id         = 'neon-gist-439401-k8'
dataset            = '1'
owner              = 'Agustín'
bucket_name        = 'datos-crudos'
temp_table_general = 'checkin_temp'

default_args = {
    'owner': owner,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Esquema de la tabla temporal para checkin.json de Yelp
temp_table_general_schema = [
    bigquery.SchemaField("business_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("date", "TIMESTAMP", mode="REPEATED"),
]

#######################################################################################
# DEFINICIÓN DEL DAG
#######################################################################################

with DAG(
    dag_id=nameDAG_base,
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    # Tarea de inicio
    inicio = DummyOperator(task_id='inicio')

    # Tarea 1: Crear la tabla temporal en BigQuery
    crear_tabla_temp = PythonOperator(
        task_id='crear_tabla_temporal',
        python_callable=crear_tabla_temporal,
        op_kwargs={
            'project_id': project_id,
            'dataset': dataset,
            'temp_table': temp_table_general,
            'schema': temp_table_general_schema
        },
    )

    # Tarea 2: Cargar el archivo checkin.json en la tabla temporal
    cargar_archivo_temp_task = PythonOperator(
        task_id='cargar_archivo_en_tabla_temporal',
        python_callable=cargar_archivo_gcs_a_bigquery,
        op_kwargs={
            'bucket_name': bucket_name,
            'file_path': 'Yelp/checkin.json',
            'project_id': project_id,
            'dataset': dataset,
            'table_name': temp_table_general
        },
    )

    # Tarea de fin
    fin = DummyOperator(task_id='fin')

    # Estructura del flujo de tareas
    inicio >> crear_tabla_temp >> cargar_archivo_temp_task >> fin
