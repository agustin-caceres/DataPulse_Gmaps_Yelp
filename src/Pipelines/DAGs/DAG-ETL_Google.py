from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
from google.cloud import storage

# Parámetros
nameDAG_base = 'Mover_Archivos_GCS_Subcarpeta'
bucket_source = 'datos-crudos'
bucket_destino = 'temporal-procesados'
subcarpeta = 'g_sitios/'  # Carpeta específica que quieres mover

default_args = {
    'owner': 'Mauricio Arce',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Función para mover archivos dentro de una subcarpeta específica
def mover_archivos():
    """Mueve los archivos dentro de una subcarpeta específica de un bucket a otro."""
    client = storage.Client()
    bucket_src = client.bucket(bucket_source)
    bucket_dest = client.bucket(bucket_destino)

    # Listar todos los blobs que estén en la subcarpeta especificada
    blobs = bucket_src.list_blobs(prefix=subcarpeta)

    for blob in blobs:
        # Copiar el archivo al bucket destino
        new_blob = bucket_dest.blob(blob.name)
        new_blob.rewrite(blob)
        
        # Eliminar el archivo del bucket de origen para completar el movimiento
        blob.delete()
        print(f"Archivo {blob.name} movido a {bucket_destino} y eliminado de {bucket_source}.")

# Definición del DAG
with DAG(
    dag_id=nameDAG_base,
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # Tareas Dummy para representar inicio y fin del proceso
    inicio = DummyOperator(task_id='inicio')

    # Tarea para mover archivos
    mover_archivos_task = PythonOperator(
        task_id='mover_archivos_g_sitios',
        python_callable=mover_archivos,
    )
    
    fin = DummyOperator(task_id='fin')

    # Estructura del flujo de tareas
    inicio >> mover_archivos_task >> fin
