from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs import GCSCopyObjectOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
from google.cloud import storage

#######################################################################################
# PARÁMETROS
#######################################################################################

nameDAG_base = 'Transferencia_Todos_Los_Archivos_GCS'
bucket_source = 'datos-crudos' 
bucket_destino = 'temporal-procesados'  

default_args = {
    'owner': 'Mauricio Arce',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

#######################################################################################
# FUNCIÓN PARA LISTAR ARCHIVOS EN UN BUCKET
#######################################################################################

def listar_archivos(bucket_name, prefix='g_sitios/'):
    """Lista todos los archivos en el bucket especificado dentro del prefijo dado y devuelve sus rutas."""
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=prefix)
    return [blob.name for blob in blobs if not blob.name.endswith('/')]  # Filtrar solo archivos, no directorios

#######################################################################################
# DEFINICIÓN DEL DAG
#######################################################################################

with DAG(
    dag_id=nameDAG_base,
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    # Tarea 1: Listar archivos en el bucket de origen
    listar_archivos_task = PythonOperator(
        task_id='listar_archivos',
        python_callable=listar_archivos,
        op_kwargs={'bucket_name': bucket_source},
        do_xcom_push=True  # Permitir que la tarea devuelva la lista de archivos
    )

    # Tarea 2: Copiar archivos a bucket de destino
    def copiar_archivos(**kwargs):
        # Obtener la lista de archivos del XCom
        archivos = kwargs['ti'].xcom_pull(task_ids='listar_archivos')
        for archivo in archivos:
            transferir_archivo = GCSCopyObjectOperator(
                task_id=f'transferir_{archivo.replace("/", "_")}',  # Crear un ID único para cada tarea
                source_bucket=bucket_source,
                source_object=archivo,
                destination_bucket=bucket_destino,
                destination_object=archivo,  # Mantiene la misma ruta en el bucket de destino
                move_object=False  # Cambiar a True si deseas mover los archivos
            )
            transferir_archivo.execute(context=kwargs)

    transferir_archivos_task = PythonOperator(
        task_id='transferir_archivos',
        python_callable=copiar_archivos,
        provide_context=True 
    )

    # Estructura del flujo de tareas
    listar_archivos_task >> transferir_archivos_task
