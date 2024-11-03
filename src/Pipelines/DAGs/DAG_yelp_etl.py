# Librerías
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
from google.cloud import bigquery

# Funciones
from functions.v2_registrar_archivo import obtener_archivos_nuevos_version_premium, registrar_archivos_en_bq
from functions.tabla_temporal import crear_tabla_temporal, cargar_archivos_en_tabla_temporal_v_premium

######################################################################################
# PARÁMETROS PARA DATOS DE YELP
######################################################################################

nameDAG_base       = 'ETL_Yelp_Checkin_to_BQ'
project_id         = 'neon-gist-439401-k8'
dataset            = '1'
owner              = 'Agustín'
GBQ_CONNECTION_ID  = 'bigquery_default'
bucket_name        = 'datos-crudos'
prefix             = 'Yelp/'
temp_table_general = 'checkin_temp'

default_args = {
    'owner': owner,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Esquema de la tabla temporal para checkin.json de Yelp
temp_table_general_schema = [
    bigquery.SchemaField("business_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("date", "STRING", mode="REPEATED"),  # Lista de fechas como strings
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

    # Tarea 1: Registrar archivos en una tabla que controla cuáles ya fueron procesados.
    registrar_archivos = PythonOperator(
        task_id='registrar_archivos_procesados',
        python_callable=obtener_archivos_nuevos_version_premium,
        op_kwargs={
            'bucket_name': bucket_name,
            'prefix': prefix,
            'project_id': project_id,
            'dataset': dataset
        },
    )

    # Tarea 2: Crear la tabla temporal en BigQuery para checkin.json
    crear_tabla_temp = PythonOperator(
        task_id='crear_tabla_temporal',
        python_callable=crear_tabla_temporal,
        op_kwargs={
            'project_id': project_id,
            'dataset': dataset,
            'temp_table': temp_table_general,
            'schema': temp_table_general_schema 
        },
    )

    # Tarea 3: Cargar el archivo en la tabla temporal
    cargar_archivo_temp_task = PythonOperator(
        task_id='cargar_archivo_en_tabla_temporal',
        python_callable=cargar_archivos_en_tabla_temporal_v_premium,
        op_kwargs={
            'bucket_name': bucket_name,
            'archivos': "{{ task_instance.xcom_pull(task_ids='registrar_archivos_procesados') }}",
            'project_id': project_id,
            'dataset': dataset,
            'temp_table': temp_table_general
        },
    )
    
    # Tarea 4: Registrar el nombre de los archivos cargados en BigQuery para control.
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
