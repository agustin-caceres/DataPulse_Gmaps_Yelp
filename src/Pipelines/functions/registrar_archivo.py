from google.cloud import bigquery
from google.cloud import storage  
from datetime import datetime

def registrar_archivos_procesados(bucket_name: str, prefix: str, project_id: str, dataset: str) -> str:
    '''
    Detecta archivos nuevos en un bucket de Google Cloud Storage y registra los archivos procesados en BigQuery.
    Esta función evita reprocesar archivos ya registrados en la tabla de BigQuery.
    
    Retorna:
    --------
    str
        Nombre del primer archivo nuevo que no se ha procesado previamente o None si no hay archivos nuevos.
    '''
    client = bigquery.Client()
    storage_client = storage.Client() 
    table_id = f"{project_id}.{dataset}.archivos_procesados"

    # Listar archivos en el bucket GCS con el prefijo especificado
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
    
    # Consulta para obtener la lista de archivos ya procesados en BigQuery
    query = f"SELECT nombre_archivo FROM `{table_id}`"
    query_job = client.query(query) 
    archivos_procesados = {row.nombre_archivo for row in query_job}
    
    # Lista de archivos actuales en el bucket de Cloud Storage con el prefijo especificado
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
    archivos = [blob.name for blob in blobs]  # Obtiene el nombre de cada archivo en el bucket

    # Filtra los archivos que aún no han sido procesados
    archivos_nuevos = [archivo for archivo in archivos if archivo not in archivos_procesados]

    # Toma solo el primer archivo nuevo, si existe
    archivo_a_procesar = archivos_nuevos[0] if archivos_nuevos else None
    
    # Si hay archivos nuevos, los registra en BigQuery
    if archivo_a_procesar:
        rows_to_insert = [{
            "nombre_archivo": archivo_a_procesar,
            "fecha_carga": datetime.now().isoformat()
        }]
        
        errors = client.insert_rows_json(table_id, rows_to_insert)
        if errors:
            print(f"Error al insertar el archivo procesado: {errors}")
        else:
             print(f"Archivo nuevo registrado exitosamente: {archivo_a_procesar}")
    else:
        print("No se encontraron archivos nuevos para procesar.")
        
    # Retorna el nombre del primer archivo nuevo detectado o None
    return archivo_a_procesar 