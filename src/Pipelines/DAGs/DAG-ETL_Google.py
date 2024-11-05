from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
from google.cloud import storage

from functions.v2_desanidar_misc import desanidar_y_mover_archivo

# Parámetros de configuración
nameDAG_base = 'DAG_Desanidar_Mover'
bucket_source = 'datos-crudos'
bucket_destino = 'temporal-procesados'
subcarpeta = 'g_sitios/'

default_args = {
    'owner': 'Mauricio Arce',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Función para obtener el primer archivo en la subcarpeta
def obtener_primer_archivo(bucket_name, prefix):
    """Obtiene el primer archivo disponible en una subcarpeta específica del bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    for blob in blobs:
        # Excluye directorios (si existieran) y retorna el primer archivo que encuentre
        if not blob.name.endswith('/'):
            return blob.name
    print("No se encontraron archivos en la subcarpeta.")
    return None

# Función para procesar solo el primer archivo encontrado
def procesar_primer_archivo(bucket_source, bucket_destino, subcarpeta):
    archivo = obtener_primer_archivo(bucket_source, subcarpeta)
    if archivo:
        print(f"Procesando archivo: {archivo}")
        desanidar_y_mover_archivo(bucket_source=bucket_source, archivo=archivo, bucket_destino=bucket_destino)
    else:
        print("No hay archivos para procesar en la subcarpeta especificada.")

# Definición del DAG
with DAG(
    dag_id=nameDAG_base,
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # Tarea para obtener y procesar solo el primer archivo en la subcarpeta
    procesar_primer_archivo_task = PythonOperator(
        task_id='procesar_primer_archivo',
        python_callable=procesar_primer_archivo,
        op_kwargs={
            'bucket_source': bucket_source,
            'bucket_destino': bucket_destino,
            'subcarpeta': subcarpeta,
        },
    )

    # Tarea Dummy para indicar el final del flujo
    fin_task = PythonOperator(
        task_id='fin_proceso',
        python_callable=lambda: print("Proceso de desanidado y movimiento de primer archivo completo."),
    )

    # Definir flujo de tareas
    procesar_primer_archivo_task >> fin_task
