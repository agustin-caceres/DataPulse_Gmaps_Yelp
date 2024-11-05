# DAG_Desanidar_Mover.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago

from functions.v2_desanidar_misc import desanidar_y_mover_archivo

# Parámetros de configuración
nameDAG_base = 'DAG_Desanidar_Mover'
bucket_source = 'datos-crudos'
bucket_destino = 'temporal-procesados'
archivo = 'g_sitios/ejemplo.json'

default_args = {
    'owner': 'Mauricio Arce',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Definición del DAG
with DAG(
    dag_id=nameDAG_base,
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # Tarea para desanidar y mover el archivo
    desanidar_y_mover_task = PythonOperator(
        task_id='desanidar_y_mover_archivo',
        python_callable=desanidar_y_mover_archivo,
        op_kwargs={
            'bucket_source': bucket_source,
            'archivo': archivo,
            'bucket_destino': bucket_destino,
        },
    )

    # Tarea Dummy para indicar el final del flujo
    fin_task = PythonOperator(
        task_id='fin_proceso',
        python_callable=lambda: print("Proceso de desanidado y movimiento completo."),
    )

    # Definir flujo de tareas
    desanidar_y_mover_task >> fin_task
