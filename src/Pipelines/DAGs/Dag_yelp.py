from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
from google.cloud import bigquery
from functions.load_data_yelp import (crear_tabla_temporal, cargar_dataframe_a_bigquery, 
                                      eliminar_tabla_temporal, archivo_procesado, 
                                      registrar_archivo_procesado)
from functions.extract_data_yelp import cargar_archivo_gcs_a_dataframe
from functions.transform_data_yelp import transformar_checkin

######################################################################################
# PARÁMETROS PARA DATOS DE YELP
######################################################################################

nameDAG_base       = 'ETL_Yelp_Checkin_to_BQ'
project_id         = 'neon-gist-439401-k8'
dataset            = '1'
owner              = 'Agustín'
bucket_name        = 'datos-crudos'
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
    bigquery.SchemaField("date", "TIMESTAMP", mode="REPEATED"),
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

    # Tarea de inicio
    inicio = DummyOperator(task_id='inicio')

    # Función para decidir el flujo en función de si el archivo fue procesado
    def decidir_flujo(**kwargs):
        if archivo_procesado(project_id, dataset, 'checkin.json'):
            return 'fin'  # Si el archivo ya fue procesado, ir a 'fin'
        else:
            return 'crear_tabla_temporal'  # Si no fue procesado, continuar el flujo

    # Tarea 1: Verificar si el archivo ya fue procesado
    verificar_archivo_procesado = BranchPythonOperator(
        task_id='verificar_archivo_procesado',
        python_callable=decidir_flujo,
    )

    # Tarea 2: Crear la tabla temporal en BigQuery (ejecuta solo si el archivo no fue procesado)
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

    # Tarea 3: Cargar el archivo checkin.json en la tabla temporal
    cargar_archivo_temp_task = PythonOperator(
        task_id='cargar_archivo_en_tabla_temporal',
        python_callable=lambda **kwargs: cargar_dataframe_a_bigquery(
            cargar_archivo_gcs_a_dataframe(bucket_name, 'Yelp/checkin.json'), 
            project_id, dataset, temp_table_general
        )
    )

    # Tarea 4: Transformar los datos y cargarlos en la tabla final
    transformar_datos = PythonOperator(
        task_id='transformar_datos_y_cargar_tabla_final',
        python_callable=transformar_checkin,
        op_kwargs={
            'project_id': project_id,
            'dataset': dataset,
            'temp_table': temp_table_general,
            'final_table': 'checkin_yelp'
        },
    )

    # Tarea 5: Eliminar la tabla temporal después de la carga en la tabla final
    eliminar_tabla_temp = PythonOperator(
        task_id='eliminar_tabla_temporal',
        python_callable=eliminar_tabla_temporal,
        op_kwargs={
            'project_id': project_id,
            'dataset': dataset,
            'table_name': temp_table_general
        },
    )

    # Tarea 6: Registrar el archivo como procesado en la tabla de control
    registrar_archivo = PythonOperator(
        task_id='registrar_archivo_procesado',
        python_callable=registrar_archivo_procesado,
        op_kwargs={
            'project_id': project_id,
            'dataset': dataset,
            'nombre_archivo': 'checkin.json'
        },
    )

    # Tarea de fin
    fin = DummyOperator(task_id='fin')

    # Estructura del flujo de tareas
    inicio >> verificar_archivo_procesado
    verificar_archivo_procesado >> crear_tabla_temp >> cargar_archivo_temp_task >> transformar_datos >> eliminar_tabla_temp >> registrar_archivo >> fin
    verificar_archivo_procesado >> fin  # Salta al final si el archivo ya fue procesado
