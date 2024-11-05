from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
from google.cloud import storage

# Parámetros
nameDAG_base = 'Transferencia_Todos_Los_Archivos_GCS'
bucket_source = 'datos-crudos'
bucket_destino = 'temporal-procesados'

default_args = {
    'owner': 'Mauricio Arce',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Función para copiar archivos
def copiar_archivos():
    """Copia todos los archivos de un bucket a otro."""
    client = storage.Client()
    bucket_src = client.bucket(bucket_source)
    bucket_dest = client.bucket(bucket_destino)

    # Listar todos los blobs en el bucket fuente
    blobs = bucket_src.list_blobs()

    for blob in blobs:
        # Crear un nuevo blob en el bucket destino con el mismo nombre
        new_blob = bucket_dest.blob(blob.name)
        new_blob.rewrite(blob)  # Copiar el archivo al nuevo blob
        print(f"Archivo {blob.name} copiado a {bucket_destino}.")

# Definición del DAG
with DAG(
    dag_id=nameDAG_base,
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # Tarea para copiar archivos
    transferir_archivos_task = PythonOperator(
        task_id='transferir_archivos',
        python_callable=copiar_archivos,
    )

    # Estructura del flujo de tareas
    transferir_archivos_task
