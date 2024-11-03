from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import timedelta
from airflow.utils.dates import days_ago

# Importar las funciones
from functions.registrar_archivo import registrar_archivos_procesados
from functions.extract_data import extract_data
from functions.load_data import load_data

#######################################################################################
# PARÁMETROS
#######################################################################################

nameDAG_base = 'Proceso_ETL_Google'
project_id = 'neon-gist-439401-k8'
dataset = '1'
owner = 'Agustín Cáceres'
bucket_name = 'datos-crudos'
prefix = 'g_sitios/'  # Prefijo de la ruta en Cloud Storage

# Definir columnas seleccionadas para extraer del JSON
selected_columns = ["category", "hours", "'MISC'", "relative_results"]

default_args = {
    'owner': owner,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
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

    # Tarea de inicio
    inicio = DummyOperator(task_id='inicio')

    # Tarea para registrar archivos procesados y obtener el nombre de archivos nuevos
    registrar_archivos = PythonOperator(
        task_id='registrar_archivos_procesados',
        python_callable=registrar_archivos_procesados,
        op_kwargs={
            'bucket_name': bucket_name,
            'prefix': prefix,
            'project_id': project_id,
            'dataset': dataset
        }
    )

    # Tarea de extracción de datos
    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract_data,
        op_kwargs={
            'bucket_name': bucket_name,
            'nombre_archivo': "{{ ti.xcom_pull(task_ids='registrar_archivos_procesados') }}",
            'selected_columns': selected_columns  # Pasamos las columnas seleccionadas
        }
    )

    # Tarea de carga de datos en BigQuery
    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load_data,
        op_kwargs={
            'flat_data': "{{ ti.xcom_pull(task_ids='extract_task') }}",
            'project_id': project_id,
            'dataset': dataset,
            'table_name': 'tablas_desanidadas'  # Nombre de la tabla en BigQuery
        }
    )

    # Tarea de finalización
    fin = DummyOperator(task_id='fin')

    # Estructura del flujo de tareas
    inicio >> registrar_archivos >> extract_task >> load_task >> fin
