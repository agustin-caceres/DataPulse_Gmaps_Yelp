from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

#######################################################################################
# PARAMETROS
#######################################################################################
nameDAG_base      = 'DAG_Cargar-datos-crudos-BQ'       # Nombre del DAG para identificar.
project_id        = 'neon-gist-439401-k8'              # ID del proyecto en cloud.
dataset           = '1'                                # ID del dataset en BigQuery.
owner             = 'Mauricio Arce'                    # Responsable del DAG.
GBQ_CONNECTION_ID = 'bigquery_default'                 # Conexión de Airflow hacia BigQuery.
bucket_name       = 'datos-crudos'                     # Nombre del bucket con los archivos crudos.

default_args = {
    'owner': owner,
    'start_date': days_ago(1),
    'retries': 1,
}

#######################################################################################
# DEFINICIÓN DEL DAG
#######################################################################################

with DAG(
    dag_id=nameDAG_base,
    default_args=default_args,
    schedule_interval=None,  # Desactivamos la programación.
    catchup=False
) as dag:

    # Dummy de inicio
    inicio = DummyOperator(task_id='inicio')

    # Listar archivos en la carpeta g_sitios
    listar_archivos = GCSListObjectsOperator(task_id='listar_archivos_en_gcs',
                                             bucket=bucket_name,)

    # Función para cargar solo nuevos archivos y definir tabla de destino
    def cargar_nuevos_archivos(**context):
        archivos = context['task_instance'].xcom_pull(task_ids='listar_archivos_en_gcs')
        archivos_procesados = obtener_archivos_procesados()  # Función para control de duplicados.
        nuevos_archivos = [archivo for archivo in archivos if archivo not in archivos_procesados]

        for archivo in nuevos_archivos:
            table_id = archivo.split('/')[-1].replace('.json', '')  # Nombre de tabla en función del archivo.
            GCSToBigQueryOperator(
                task_id=f'cargar_{table_id}_a_bigquery',
                bucket=bucket_name,
                source_objects=[archivo],
                destination_project_dataset_table=f'{project_id}.{dataset}.{table_id}',  # Dataset y tabla dinámicos.
                source_format='NEWLINE_DELIMITED_JSON',
                write_disposition='WRITE_APPEND',
                gcp_conn_id=GBQ_CONNECTION_ID
            ).execute(context=context)

    carga_incremental = PythonOperator(
        task_id='cargar_nuevos_archivos',
        python_callable=cargar_nuevos_archivos,
        provide_context=True
    )

    fin = DummyOperator(task_id='fin')

    # Flujo de ejecucion de tareas.
    inicio >> listar_archivos >> carga_incremental >> fin
