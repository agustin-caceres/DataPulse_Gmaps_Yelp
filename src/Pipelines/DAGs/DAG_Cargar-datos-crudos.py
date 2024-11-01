from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from google.cloud import bigquery
from datetime import timedelta
from datetime import datetime

#######################################################################################
# PARÁMETROS
#######################################################################################

nameDAG_base      = 'DAG_Cargar-datos-crudos-BQ'           # Nombre del DAG para identificar.
project_id        = 'neon-gist-439401-k8'                  # ID del proyecto en Google Cloud.
dataset           = '1'                                    # ID del dataset en BigQuery.
email             = ['agusca.saot@gmail.com']              # Email de notificación.
owner             = 'Mauricio Arce'                        # Responsable del DAG.
GBQ_CONNECTION_ID = 'bigquery_default'                     # Conexión de Airflow hacia BigQuery.
bucket_name       = 'datos-crudos'                         # Nombre del bucket con los archivos crudos.

default_args = {
    'owner': owner,
    'email': email,
    'email_on_failure': True,
    'email_on_retry': True,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#######################################################################################
# FUNCIONES
#######################################################################################

def obtener_archivos_procesados() -> list:
    """Obtiene la lista de archivos ya procesados desde BigQuery.
    
    Returns:
        list: Lista de nombres de archivos ya procesados.
    """
    client = bigquery.Client()
    query = f"""
        SELECT nombre_archivo 
        FROM `{project_id}.{dataset}.archivos_procesados`
    """
    query_job = client.query(query)
    records = query_job.result()
    return [record.nombre_archivo for record in records]

def registrar_archivo_procesado(nombre_archivo: str) -> None:
    """Registra un archivo como procesado en BigQuery.
    
    Args:
        nombre_archivo (str): Nombre del archivo procesado.
    """
    client = bigquery.Client()
    table_id = f"{project_id}.{dataset}.archivos_procesados"
    
    # Inserción del nuevo registro
    rows_to_insert = [
        {"nombre_archivo": nombre_archivo,
         "fecha_carga": datetime.now()}  
    ]
    
    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        print(f"Error al insertar el archivo procesado: {errors}")

def cargar_nuevos_archivos(**context) -> None:
    """Carga archivos nuevos desde GCS a BigQuery, evitando duplicados y registrando archivos procesados.
    
    Args:
        context (dict): Contexto de Airflow para acceder a XCom.
    """
    # Obtener la lista de archivos en GCS
    archivos = context['task_instance'].xcom_pull(task_ids='listar_archivos_en_gcs')
    
    # Obtener la lista de archivos ya procesados desde BigQuery
    archivos_procesados = obtener_archivos_procesados()
    
    # Filtrar archivos que no han sido procesados
    nuevos_archivos = [archivo for archivo in archivos if archivo not in archivos_procesados]

    # Procesar cada nuevo archivo
    for archivo in nuevos_archivos:
        # Construcción del ID de tabla en función del nombre y carpeta de origen
        path, file_name = archivo.split('/')
        table_id = path  # Usar el nombre de la carpeta como ID de tabla

        # Determinar el formato de archivo
        if archivo.endswith('.json'):
            source_format = 'NEWLINE_DELIMITED_JSON'
        elif archivo.endswith('.csv'):
            source_format = 'CSV'
        elif archivo.endswith('.parquet'):
            source_format = 'PARQUET'
        else:
            print(f"Formato no soportado para el archivo: {archivo}")
            continue  # Salta al siguiente archivo si el formato no es soportado

        # Crear tarea para cargar archivo a BigQuery
        cargar_a_bigquery = GCSToBigQueryOperator(
            task_id=f'cargar_{file_name.replace(".", "_")}_a_bigquery',  # Tarea por archivo
            bucket=bucket_name,
            source_objects=[archivo],
            destination_project_dataset_table=f'{project_id}.{dataset}.{table_id}',  # Tabla destino común
            source_format=source_format,
            write_disposition='WRITE_APPEND',
            gcp_conn_id=GBQ_CONNECTION_ID,
            trigger_rule='one_success'  # Asegura que la tarea solo se ejecute si hay un éxito previo
        )
        
        # Se ejecuta la carga
        context['ti'].xcom_push(key='cargar_task', value=cargar_a_bigquery)

        # Esperar la finalización de la tarea
        cargar_a_bigquery.execute(context=context)

        # Registrar el archivo procesado
        registrar_archivo_procesado(file_name)

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
        bucket=bucket_name,
        # Especificar el output como xcom
        xcom_push=True,
    )
    
    obtener_archivo_procesado = PythonOperator(
        task_id='obtener_archivos_procesados',
        python_callable=obtener_archivos_procesados,
        provide_context=True
    )
    
    cargar_archivos = PythonOperator(
        task_id='cargar_nuevos_archivos',
        python_callable=cargar_nuevos_archivos,
        provide_context=True
    )

    fin = DummyOperator(task_id='fin')

    # Estructura del flujo de tareas
    inicio >> listar_archivos >> obtener_archivo_procesado >> cargar_archivos >> fin

