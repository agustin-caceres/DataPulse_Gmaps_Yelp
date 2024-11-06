# Librerias
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import timedelta
from airflow.utils.dates import days_ago

#Funciones
from functions.registrar_archivo import registrar_archivos_procesados
from functions.desanidar_misc import desanidar_misc, actualizar_misc_con_atributos, eliminar_categorias_especificas, generalizar_atributos, marcar_nuevas_accesibilidades

######################################################################################
# PARÁMETROS 1
######################################################################################

nameDAG_base      = 'Procesamiento_ETL_Google'
project_id        = 'neon-gist-439401-k8'
dataset           = '1'
owner             = 'Mauricio Arce'
GBQ_CONNECTION_ID = 'bigquery_default'
bucket_name       = 'datos-crudos'

default_args = {
    'owner': owner,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

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

    # Tarea 1: Registrar archivos procesados y devolver el nombre del primer archivo nuevo
#    registrar_archivos = PythonOperator(
 #       task_id='registrar_archivos_procesados',
  #      python_callable=registrar_archivos_procesados,
   #     op_kwargs={
    #        'bucket_name': bucket_name,
     #       'prefix': 'g_sitios/',
      #      'project_id': project_id,
       #     'dataset': dataset
        #}
    #)

#    # Tarea 2: Desanidar el archivo de datos 'MISC' usando el nombre del archivo del XCom
#    desanidar_misc_task = PythonOperator(
 #       task_id='desanidar_misc',
  #      python_callable=desanidar_misc,
   #     op_kwargs={
    #        'bucket_name': bucket_name,
     #       'archivo': "{{ ti.xcom_pull(task_ids='registrar_archivos_procesados') }}",
        #    'project_id': project_id,
      #      'dataset': dataset
       # }
    #)
    
    # Tarea 3: Actualizar la tabla con nuevas columnas 'category', 'misc_content' y 'atributo'
#    actualizar_misc_task = PythonOperator(
#        task_id='actualizar_misc_con_atributos',
#        python_callable=actualizar_misc_con_atributos,
#        op_kwargs={
#            'project_id': project_id,
#            'dataset': dataset
#        }
#    ) 
    
    # Tarea 4: Elimina las categorias que no se van a utilizar.
#    eliminar_categorias = PythonOperator(
#        task_id="eliminar_categorias_especificas",
#        python_callable=eliminar_categorias_especificas,
#        op_kwargs={
#            'project_id': project_id,
#            'dataset': dataset
#            }
#    )

    # Tarea 5: Generalizar los atributos
#    generalizar_atributos_task = PythonOperator(
#        task_id="generalizar_atributos",
#        python_callable=generalizar_atributos,
#        op_kwargs={
#            'project_id': project_id,
#            'dataset': dataset
#        },
#    )
 
    # Tarea 6 marcar atributos sensibles como accesibilidades
    añadir_accesibilidades = PythonOperator(
        task_id="añadir_accesibilidades",
        python_callable=marcar_nuevas_accesibilidades,
        op_kwargs={
            'project_id': project_id,
            'dataset': dataset
        },
    )
  
    fin = DummyOperator(task_id='fin')  
  
    # Estructura del flujo de tareas  
    inicio >> añadir_accesibilidades  >> fin

