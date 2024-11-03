# Librerias
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
from google.cloud import bigquery

# Funciones
from functions.v2_registrar_archivo import obtener_archivos_nuevos, registrar_archivos_en_bq
from functions.tabla_temporal import crear_tabla_temporal, cargar_archivos_en_tabla_temporal

######################################################################################
# PARÁMETROS
######################################################################################

nameDAG_base       = 'ETL_Storage_to_BQ'
project_id         = 'neon-gist-439401-k8'
dataset            = '1'
owner              = 'Mauricio Arce'
GBQ_CONNECTION_ID  = 'bigquery_default'
bucket_name        = 'datos-crudos'
prefix             = 'g_sitios/'
temp_table_general = 'data_cruda'

default_args = {
    'owner': owner,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Tabla temporal de la metadata cruda que se va a desanidar y procesar.
temp_table_general_schema = [
    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("address", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("gmap_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("description", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("latitude", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("longitude", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("category", "STRING", mode="REPEATED"),  # Asume una lista de strings
    bigquery.SchemaField("avg_rating", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("num_of_reviews", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("price", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("hours", "STRING", mode="NULLABLE"),     # Almacena las horas como texto JSON
    bigquery.SchemaField("MISC", "STRING", mode="NULLABLE"),      # Almacena MISC como texto JSON
    bigquery.SchemaField("state", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("relative_results", "STRING", mode="NULLABLE"),  # Almacena como texto JSON
    bigquery.SchemaField("url", "STRING", mode="NULLABLE"),
]

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

    # Tarea 1: Registrar archivos en una tabla que controlara cuales ya fueron procesados y cuales no.
    registrar_archivos = PythonOperator(
        task_id='registrar_archivos_procesados',
        python_callable=obtener_archivos_nuevos,
        op_kwargs={
            'bucket_name': bucket_name,
            'prefix': prefix,
            'project_id': project_id,
            'dataset': dataset
        },
        provide_context=True,
    )

    # Tarea 2: Crear la tabla temporal en BigQuery de todo el Json.
    crear_tabla_temp = PythonOperator(
        task_id='crear_tabla_temporal',
        python_callable=crear_tabla_temporal,
        op_kwargs={
            'project_id': project_id,
            'dataset': dataset,
            'temp_table': temp_table_general,
            'schema': temp_table_general_schema 
        },
        provide_context=True,
    )

   # Tarea 3: Cargar los archivos JSON en la tabla temporal
    cargar_archivo_temp_task = PythonOperator(
        task_id='cargar_archivo_en_tabla_temporal',
        python_callable=cargar_archivos_en_tabla_temporal,
        op_kwargs={
            'bucket_name': bucket_name,
            'archivos': '{{ task_instance.xcom_pull(task_ids="registrar_archivos_procesados") }}',  # Esto recupera la lista de archivos
            'project_id': project_id,
            'dataset': dataset,
            'temp_table': temp_table_general
        },
        on_failure_callback=lambda context: print(f"Error en la tarea: {context['task_instance'].task_id}"),
    )
    
    # Tarea 4: Registrar el nombre de los archivos cargados en BigQuery, para control.
    registrar_archivo_en_bq = PythonOperator(
        task_id='registrar_archivo_en_bq',
        python_callable=registrar_archivos_en_bq,
        op_kwargs={
            'project_id': project_id,
            'dataset': dataset,
            'archivos_nuevos': "{{ ti.xcom_pull(task_ids='registrar_archivos_procesados') }}"
        },
    )

    fin = DummyOperator(task_id='fin')

    # Estructura del flujo de tareas
    inicio >> registrar_archivos >> crear_tabla_temp >> cargar_archivo_temp_task >> registrar_archivo_en_bq >> fin
