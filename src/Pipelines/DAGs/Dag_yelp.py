from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
from google.cloud import bigquery
from functions.load_data_yelp import (crear_tabla_temporal, cargar_dataframe_a_bigquery, 
                                      eliminar_tabla_temporal, archivo_procesado, 
                                      registrar_archivo_procesado, obtener_fecha_actualizacion)
from functions.extract_data_yelp import cargar_archivo_gcs_a_dataframe
from functions.transform_data_yelp import transformar_checkin, transformar_tip

######################################################################################
# PARÁMETROS PARA DATOS DE YELP
######################################################################################

nameDAG_base = 'ETL_Yelp'
project_id = 'neon-gist-439401-k8'
dataset = '1'
owner = 'Agustín'
bucket_name = 'datos-crudos'

default_args = {
    'owner': owner,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Esquemas temporales para checkin.json y tip.json
temp_table_checkin = 'checkin_temp'
temp_table_tip = 'tip_temp'

schema_checkin = [
    bigquery.SchemaField("business_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("date", "TIMESTAMP", mode="REPEATED"),
]

schema_tip = [
    bigquery.SchemaField("text", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("compliment_count", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("business_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
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

    def decidir_flujo(archivo_nombre, **kwargs):
        # Obtén la fecha de actualización del archivo desde el bucket en GCS
        fecha_actualizacion = obtener_fecha_actualizacion(bucket_name, archivo_nombre)
        
        # Pasa `fecha_actualizacion` al verificar si el archivo ya fue procesado
        if archivo_procesado(project_id, dataset, archivo_nombre, fecha_actualizacion):
            # Retorna el nombre exacto del DummyOperator de fin correspondiente
            return f'fin_{archivo_nombre.split(".")[0]}'  # Simplifica para que coincida con los nombres de los DummyOperators
        else:
            # Retorna el nombre exacto de la tarea de creación de tabla temporal
            return f'crear_tabla_temporal_{archivo_nombre.split(".")[0]}'

    # Verificación y flujo condicional para cada archivo
    verificar_checkin = BranchPythonOperator(
        task_id='verificar_archivo_procesado_checkin',
        python_callable=decidir_flujo,
        op_kwargs={'archivo_nombre': 'checkin.json'},
    )

    verificar_tip = BranchPythonOperator(
        task_id='verificar_archivo_procesado_tip',
        python_callable=decidir_flujo,
        op_kwargs={'archivo_nombre': 'tip.json'},
    )

    # Procesamiento de checkin.json
    crear_tabla_checkin = PythonOperator(
        task_id='crear_tabla_temporal_checkin',
        python_callable=crear_tabla_temporal,
        op_kwargs={
            'project_id': project_id,
            'dataset': dataset,
            'temp_table': temp_table_checkin,
            'schema': schema_checkin
        },
    )

    cargar_checkin = PythonOperator(
        task_id='cargar_archivo_checkin',
        python_callable=lambda **kwargs: cargar_dataframe_a_bigquery(
            cargar_archivo_gcs_a_dataframe(bucket_name, 'Yelp/checkin.json'), 
            project_id, dataset, temp_table_checkin
        )
    )

    transformar_checkin_task = PythonOperator(
        task_id='transformar_checkin',
        python_callable=transformar_checkin,
        op_kwargs={
            'project_id': project_id,
            'dataset': dataset,
            'temp_table': temp_table_checkin,
            'final_table': 'checkin_yelp'
        },
    )

    eliminar_checkin = PythonOperator(
        task_id='eliminar_tabla_temporal_checkin',
        python_callable=eliminar_tabla_temporal,
        op_kwargs={
            'project_id': project_id,
            'dataset': dataset,
            'table_name': temp_table_checkin
        },
    )

    registrar_checkin = PythonOperator(
        task_id='registrar_archivo_checkin',
        python_callable=registrar_archivo_procesado,
        op_kwargs={
            'project_id': project_id,
            'dataset': dataset,
            'nombre_archivo': 'checkin.json'
        },
    )

    fin_checkin = DummyOperator(task_id='fin_checkin')

    # Procesamiento de tip.json
    crear_tabla_tip = PythonOperator(
        task_id='crear_tabla_temporal_tip',
        python_callable=crear_tabla_temporal,
        op_kwargs={
            'project_id': project_id,
            'dataset': dataset,
            'temp_table': temp_table_tip,
            'schema': schema_tip
        },
    )

    cargar_tip = PythonOperator(
        task_id='cargar_archivo_tip',
        python_callable=lambda **kwargs: cargar_dataframe_a_bigquery(
            cargar_archivo_gcs_a_dataframe(bucket_name, 'Yelp/tip.json'), 
            project_id, dataset, temp_table_tip
        )
    )

    transformar_tip_task = PythonOperator(
        task_id='transformar_tip',
        python_callable=transformar_tip,
        op_kwargs={
            'project_id': project_id,
            'dataset': dataset,
            'temp_table': temp_table_tip,
            'final_table': 'tip_yelp'
        },
    )

    eliminar_tip = PythonOperator(
        task_id='eliminar_tabla_temporal_tip',
        python_callable=eliminar_tabla_temporal,
        op_kwargs={
            'project_id': project_id,
            'dataset': dataset,
            'table_name': temp_table_tip
        },
    )

    registrar_tip = PythonOperator(
        task_id='registrar_archivo_tip',
        python_callable=registrar_archivo_procesado,
        op_kwargs={
            'project_id': project_id,
            'dataset': dataset,
            'nombre_archivo': 'tip.json'
        },
    )

    fin_tip = DummyOperator(task_id='fin_tip')

    # Estructura de flujo para checkin.json
    inicio >> verificar_checkin
    verificar_checkin >> crear_tabla_checkin >> cargar_checkin >> transformar_checkin_task >> eliminar_checkin >> registrar_checkin >> fin_checkin
    verificar_checkin >> fin_checkin

    # Estructura de flujo para tip.json
    inicio >> verificar_tip
    verificar_tip >> crear_tabla_tip >> cargar_tip >> transformar_tip_task >> eliminar_tip >> registrar_tip >> fin_tip
    verificar_tip >> fin_tip
