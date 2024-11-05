# Librerias
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.operators.dummy import DummyOperator
from datetime import timedelta
from airflow.utils.dates import days_ago

# Funciones
from functions.v2_desanidar_misc import procesar_archivos

###################################################################################### 
# PARÁMETROS 
###################################################################################### 

nameDAG_base         = 'ETL_Storage_to_BQ5'            # Nombre del DAG en Airflow.
project_id           = 'neon-gist-439401-k8'           # ID del proyecto en Cloud.
dataset              = '1'                             # ID dataset en BigQuery.
owner                = 'Mauricio Arce'                 # Responsable del DAG.
GBQ_CONNECTION_ID    = 'bigquery_default'              # Conexion de Airflow hacia BigQuery.
bucket_no_procesados = 'datos-crudos'                  # Bucket de los archivos no procesados.
prefix               = 'g_sitios/'                     # Carpeta donde se encuentran los archivos dentro del bucket.
bucket_procesados    = 'temporal-procesados'           # Bucket donde se transferira los archivos no procesados una vez cargados en BQ.
tabla_temporal       = 'tabla-temporal'                # Tabla temporal en Bigquery donde se subiran los archivos crudos para procesarlos. 

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
    
    # Tarea 1: Listar los archivos en el bucket de entrada
    listar_archivos = GCSListObjectsOperator(
    task_id='listar_archivos',
    bucket=bucket_no_procesados,
    prefix=prefix, 
    )
    
    # Tarea 2: Procesar todos los archivos
    procesar_archivos_task = PythonOperator(
        task_id='procesar_archivos',
        python_callable=procesar_archivos,
        op_kwargs={
            'bucket_entrada': bucket_no_procesados,
            'bucket_procesado': bucket_procesados,
            'prefix': prefix,
            'archivos': "{{ task_instance.xcom_pull(task_ids='listar_archivos') }}",
        },
    )
    # Tarea 3: Subir los archivos procesados a una tabla temporal en BigQuery
    subir_a_bq_task = GCSToBigQueryOperator(
        task_id='subir_a_bq',
        bucket=bucket_procesados,              
        source_objects=['*'],                    
        destination_project_dataset_table=f"{project_id}.{dataset}.{tabla_temporal}", 
        source_format='NEWLINE_DELIMITED_JSON',
        write_disposition='WRITE_APPEND', 
    )

    fin = DummyOperator(task_id='fin')

    # Flujo de tareas.
    inicio >> listar_archivos >> procesar_archivos_task >> subir_a_bq_task >> fin