# Librerias
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import timedelta
from airflow.utils.dates import days_ago

#Funciones
from functions.registrar_archivo import detectar_archivos_nuevos, registrar_archivo_exitoso
from functions.google_bigquery import crear_tablas_bigquery,eliminar_tablas_temporales 
from functions.desanidar_misc import desanidar_misc,actualizar_misc_con_atributos,generalizar_atributos, eliminar_categorias_especificas, marcar_nuevas_accesibilidades, mover_a_tabla_oficial 
from functions.desanidar_rr import desanidar_relative_results

######################################################################################
# PARÁMETROS
######################################################################################

nameDAG_base      = 'Procesamiento_ETL_Google'
project_id        = 'neon-gist-439401-k8'
dataset           = '1'
owner             = 'Mauricio Arce'
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
    description='Desanida y crea las tablas utilizadas en google.',
    schedule_interval=None,
    catchup=False
) as dag:

    inicio = DummyOperator(task_id='inicio')

    # Tarea 1: Detectar archivos ya procesados y devolver el nombre del primer archivo aun no procesado
    detectar_archivos_task = PythonOperator(
        task_id='detectar_archivos',
        python_callable=detectar_archivos_nuevos,
      op_kwargs={
            'bucket_name': bucket_name,
            'prefix': 'g_sitios/',
            'project_id': project_id,
            'dataset': dataset
        }
    )
    
    # Tarea 2: Crear tablas temporales en BigQuery
    crear_tablas_temporales_task = PythonOperator(
        task_id="crear_tablas_temporales",
        python_callable=crear_tablas_bigquery,
        op_kwargs={
            'project_id': project_id,
            'dataset': dataset
        },
    )
    
    # Tarea 3: Desanidar el archivo de datos 'MISC' usando el nombre del archivo del XCom
    desanidar_misc_task = PythonOperator(
        task_id='desanidar_misc',
        python_callable=desanidar_misc,
        op_kwargs={
            'bucket_name': bucket_name,
            'archivo': "{{ ti.xcom_pull(task_ids='detectar_archivos') }}",
            'project_id': project_id,
            'dataset': dataset
        }
    )
    
    # Tarea 4: Desanidar el archivo de datos 'relative_results' usando el nombre del archivo del XCom
    desanidar_rr_task = PythonOperator(
        task_id='desanidar_rr',
        python_callable=desanidar_relative_results,
        op_kwargs={
            'bucket_name': bucket_name,
            'archivo': "{{ ti.xcom_pull(task_ids='detectar_archivos') }}",
            'project_id': project_id,
            'dataset': dataset
        }
    )
    '''
    # Tarea 4: Actualizar la tabla con nuevas columnas 'category', 'misc_content' y 'atributo'
    actualizar_misc_task = PythonOperator(
        task_id='actualizar_misc_con_atributos',
        python_callable=actualizar_misc_con_atributos,
        op_kwargs={
            'project_id': project_id,
            'dataset': dataset
        }
    ) 
    
    # Tarea 5: Elimina las categorias que no se van a utilizar.
    eliminar_categorias_task = PythonOperator(
        task_id="eliminar_categorias_especificas",
        python_callable=eliminar_categorias_especificas,
        op_kwargs={
            'project_id': project_id,
            'dataset': dataset
            }
    )

    # Tarea 6: Generalizar los atributos
    generalizar_atributos_task = PythonOperator(
        task_id="generalizar_atributos",
        python_callable=generalizar_atributos,
        op_kwargs={
            'project_id': project_id,
            'dataset': dataset
        },
    )
 
    # Tarea 7: marcar atributos sensibles como accesibilidades
    anadir_accesibilidades_task = PythonOperator(
        task_id="anadir_accesibilidades",
        python_callable=marcar_nuevas_accesibilidades,
        op_kwargs={
            'project_id': project_id,
            'dataset': dataset
        },
    )
  
    # Tarea 8: Mover los datos de la tabla temporal a la tabla oficial
    mover_a_tabla_oficial_task = PythonOperator(
        task_id="mover_a_tabla_oficial",
        python_callable=mover_a_tabla_oficial,
        op_kwargs={
            'project_id': project_id,
            'dataset': dataset
        },
    )

    # Tarea 9: Eliminar las tablas temporales
    eliminar_tablas_temporales_task = PythonOperator(
        task_id="eliminar_tablas_temporales",
        python_callable=eliminar_tablas_temporales,
        op_kwargs={
            'project_id': project_id,
            'dataset': dataset
        },
    )
    
    registrar_archivo_procesado_task = PythonOperator(
        task_id="registrar_archivo_procesado",
        python_callable=registrar_archivo_exitoso,
        op_kwargs={
            'archivo': "{{ task_instance.xcom_pull(task_ids='detectar_archivos') }}",
            'project_id': project_id,
            'dataset': dataset
        }
    )
    '''
    fin = DummyOperator(task_id='fin')
    
    # Estructura del flujo de tareas  
    #inicio >> detectar_archivos_task >> crear_tablas_temporales_task >> desanidar_misc_task >> actualizar_misc_task >> eliminar_categorias_task >> generalizar_atributos_task >> anadir_accesibilidades_task >>  mover_a_tabla_oficial_task >> eliminar_tablas_temporales_task >> registrar_archivo_procesado_task >> fin
    inicio >> detectar_archivos_task >> crear_tablas_temporales_task
    crear_tablas_temporales_task >> [desanidar_misc_task, desanidar_rr_task]
    [desanidar_misc_task, desanidar_rr_task] >> fin