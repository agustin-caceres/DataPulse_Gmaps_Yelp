''' 
   # Tarea 4: Desanidar misc y procesar el archivo en la tabla temporal
    procesar_misc_task = PythonOperator(
        task_id='desanidar_misc',
        python_callable=desanidar_misc,
        op_kwargs={
            'bucket_name': bucket_name,
            'archivo': "{{ ti.xcom_pull(task_ids='registrar_archivos_procesados') }}",
            'project_id': project_id,
            'dataset': dataset,
            'temp_table': temp_table  # Procesar en la tabla temporal
        }
    )


    # Tarea 5: Mover datos de la tabla temporal a la final y eliminar la temporal
    mover_datos_y_borrar_temp_task = PythonOperator(
        task_id='mover_datos_y_borrar_temp',
        python_callable=mover_datos_y_borrar_temp,
        op_kwargs={
            'project_id': project_id,
            'dataset': dataset,
            'temp_table': temp_table,
            'final_table': final_table
        }
    )
'''