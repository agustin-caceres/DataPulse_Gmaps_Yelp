# Librerias
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.dummy import DummyOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
from google.cloud import bigquery

# Funciones
#from functions.funcion import alguna funcion que use

######################################################################################
# PARÁMETROS
######################################################################################

nameDAG_base         = 'ETL_Storage_to_BQ'      # Nombre del DAG en Airflow.
project_id           = 'neon-gist-439401-k8'    # ID del proyecto en Cloud.
dataset              = '1'                      # ID dataset en BigQuery.
owner                = 'Mauricio Arce'          # Responsable del DAG.
GBQ_CONNECTION_ID    = 'bigquery_default'       # Conexion de Airflow hacia BigQuery.
GCS_CONNECTION_ID    = 'google_cloud_default'   # Conexion de Airflow hacia Storage.
bucket_no_procesados = 'datos-crudos'           # Bucket de los archivos no procesados.
prefix               = 'g_sitios/'              # Carpeta donde se encuentran los archivos dentro del bucket.
bucket_procesados    = 'temporal-procesados'    # Bucket donde se transferira los archivos no procesados una vez cargados en BQ.
tabla_temporal       = 'tabla-temporal'         # Tabla temporal en Bigquery donde se subiran los archivos crudos para procesarlos. 

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
    dag_id=nameDAG_base,
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    inicio = DummyOperator(task_id='inicio')

    # Tarea 1: Carga los archivos del bucket de no-procesados a bigquery.
    cargar_archivos_no_procesados = GCSToBigQueryOperator(
    task_id='cargar_archivos_no_procesados',
    bucket=bucket_no_procesados,
    source_objects=[prefix + '*.json'],
    destination_project_dataset_table=f'{project_id}.{dataset}.{tabla_temporal}',
    source_format='NEWLINE_DELIMITED_JSON',  
    write_disposition='WRITE_TRUNCATE',
    bigquery_conn_id=GBQ_CONNECTION_ID,
    google_cloud_storage_conn_id=GCS_CONNECTION_ID,
    autodetect=True
)


    # Tarea 2: Verificar que el archivo se haya cargado correctamente
    validacion_carga = 'BigQueryCheckOperator('
    
    
   # Tarea 3: Mover el archivo al bucket de procesados si la validación es exitosa
    mover_a_procesados = 'GCSToGCSOperator('
    
    # Tarea 4: Registrar el nombre de los archivos cargados en BigQuery, para control. en una tabla de archivos_procesados.
    registrar_archivo_en_bq = 'PythonOperator('

    fin = DummyOperator(task_id='fin')

    # Estructura del flujo de tareas
    inicio >> cargar_archivos_no_procesados >> fin
    #inicio >> cargar_archivos_no_procesados >> validacion_carga >> mover_a_procesados >>  registrar_archivo_en_bq >> fin