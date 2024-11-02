from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from google.cloud import bigquery
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
import datetime

#######################################################################################
# PARÁMETROS
#######################################################################################

nameDAG_base      = 'DAG_Cargar-datos-crudos-BQ'           # Nombre del DAG para identificar.
project_id        = 'neon-gist-439401-k8'                  # ID del proyecto en Google Cloud.
dataset           = '1'                                    # ID del dataset en BigQuery.
owner             = 'Mauricio Arce'                        # Responsable del DAG.
GBQ_CONNECTION_ID = 'bigquery_default'                     # Conexión de Airflow hacia BigQuery.
bucket_name       = 'datos-crudos'                         # Nombre del bucket con los archivos crudos.

default_args = {
    'owner': owner,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#######################################################################################
# FUNCIONES
#######################################################################################

def obtener_archivos_procesados() -> list:
    client = bigquery.Client()
    query = f"""
        SELECT nombre_archivo 
        FROM `{project_id}.{dataset}.archivos_procesados`
    """
    query_job = client.query(query)
    records = query_job.result()
    return [record.nombre_archivo for record in records]

def registrar_archivo_procesado(nombre_archivo: str) -> None:
    client = bigquery.Client()
    table_id = f"{project_id}.{dataset}.archivos_procesados"
    
    rows_to_insert = [
        {"nombre_archivo": nombre_archivo,
         "fecha_carga": datetime.datetime.now()}  
    ]
    
    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        print(f"Error al insertar el archivo procesado: {errors}")

def filtrar_nuevos_archivos(**context) -> list:
    archivos = context['task_instance'].xcom_pull(task_ids='listar_archivos_en_gcs')
    archivos_procesados = obtener_archivos_procesados()
    nuevos_archivos = [archivo for archivo in archivos if archivo not in archivos_procesados]
    return nuevos_archivos  # Devuelve la lista de archivos nuevos para XCom

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
    
    listar_archivos = GCSListObjectsOperator(
        task_id='listar_archivos_en_gcs',
        bucket=bucket_name
    )
    
    obtener_nuevos_archivos = PythonOperator(
        task_id='obtener_nuevos_archivos',
        python_callable=filtrar_nuevos_archivos,
        provide_context=True
    )

    # Definir el TaskGroup para cargar archivos nuevos a BigQuery
    def crear_tareas_carga(archivos, **context):
        with TaskGroup(group_id='cargar_archivos_group', tooltip='Carga de archivos a BigQuery') as cargar_archivos_group:
            for archivo in archivos:
                path, file_name = archivo.split('/')
                table_id = path
                
                if archivo.endswith('.json'):
                    source_format = 'NEWLINE_DELIMITED_JSON'
                elif archivo.endswith('.csv'):
                    source_format = 'CSV'
                elif archivo.endswith('.parquet'):
                    source_format = 'PARQUET'
                else:
                    print(f"Formato no soportado para el archivo: {archivo}")
                    continue
                
                cargar_a_bigquery = GCSToBigQueryOperator(
                    task_id=f'cargar_{file_name.replace(".", "_")}_a_bigquery',
                    bucket=bucket_name,
                    source_objects=[archivo],
                    destination_project_dataset_table=f'{project_id}.{dataset}.{table_id}',
                    source_format=source_format,
                    write_disposition='WRITE_APPEND',
                    gcp_conn_id=GBQ_CONNECTION_ID,
                )
                
                cargar_a_bigquery >> PythonOperator(
                    task_id=f'registrar_{file_name.replace(".", "_")}_procesado',
                    python_callable=registrar_archivo_procesado,
                    op_kwargs={'nombre_archivo': file_name},
                )
        return cargar_archivos_group

    # Crear tarea para ejecutar el TaskGroup con los archivos nuevos
    cargar_archivos_task = PythonOperator(
        task_id='cargar_archivos_task',
        python_callable=crear_tareas_carga,
        op_kwargs={'archivos': obtener_nuevos_archivos.output}  # Usamos XCom para pasar la lista de archivos nuevos
    )

    fin = DummyOperator(task_id='fin')

    # Estructura del flujo de tareas
    inicio >> listar_archivos >> obtener_nuevos_archivos >> cargar_archivos_task >> fin
