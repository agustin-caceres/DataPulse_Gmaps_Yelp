# Librerias
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
from google.cloud import storage

# Funciones
from functions.mover_archivos import mover_archivos
######################################################## PARAMETROS

nameDAG_base        = 'Mover_Archivos_GCS_Subcarpeta'
bucket_source       = 'datos-crudos'
bucket_destino      = 'temporal-procesados'
subcarpeta          = 'g_sitios/'  

default_args = {
    'owner': 'Mauricio Arce',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

######################################################## DAG

# Definición del DAG
with DAG(
    dag_id=nameDAG_base,
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    inicio = DummyOperator(task_id='inicio')

    # Tarea 1: Mover archivos de un bucket a otro
    mover_archivos_task = PythonOperator(
        task_id='mover_archivos_g_sitios',
        python_callable=mover_archivos,
        op_args=[bucket_destino, bucket_source, subcarpeta],
    )
    
    fin = DummyOperator(task_id='fin')

    # Estructura del flujo de tareas
    inicio >> mover_archivos_task >> fin
