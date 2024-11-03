from google.cloud import bigquery, storage  
from datetime import datetime
import logging

def obtener_archivos_nuevos(bucket_name: str, prefix: str, project_id: str, dataset: str) -> list:
    """
    Detecta archivos nuevos en un bucket de Google Cloud Storage comparando con los archivos ya registrados en BigQuery.
    
    Args:
        bucket_name (str): Nombre del bucket de Cloud Storage.
        prefix (str): Prefijo para filtrar los archivos en el bucket.
        project_id (str): ID del proyecto de Google Cloud.
        dataset (str): Nombre del dataset de BigQuery.

    Returns:
        list: Lista de archivos nuevos detectados.
    """
    if not all([bucket_name, prefix, project_id, dataset]):
        raise ValueError("Todos los parámetros deben ser proporcionados y no pueden estar vacíos.")

    try:
        # Inicializa el cliente de BigQuery y de Cloud Storage
        client = bigquery.Client()
        storage_client = storage.Client() 

        # Define el ID de la tabla de BigQuery en formato "project.dataset.table"
        table_id = f"{project_id}.{dataset}.archivos_procesados"
        
        # Consulta para obtener la lista de archivos ya procesados en BigQuery
        query = f"SELECT nombre_archivo FROM `{table_id}`"
        query_job = client.query(query)
        query_job.result()  # Espera a que la consulta se complete
        archivos_procesados = {row.nombre_archivo for row in query_job}

        # Lista de archivos actuales en el bucket de Cloud Storage con el prefijo especificado
        blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
        archivos = [blob.name for blob in blobs]

        # Filtra los archivos que aún no han sido procesados
        archivos_nuevos = [archivo for archivo in archivos if archivo not in archivos_procesados]
        print(f"Archivos nuevos detectados: {archivos_nuevos}")

        return archivos_nuevos
    
    except Exception as e:
        print(f"Error en obtener_archivos_nuevos: {e}")
        return []  # Retorna una lista vacía en caso de error


def registrar_archivos_en_bq(project_id: str, dataset: str, archivos_nuevos: list) -> None:
    """
    Registra archivos nuevos en la tabla 'archivos_procesados' de BigQuery.
    
    Parámetros:
    -----------
    project_id : str
        ID del proyecto en Google Cloud Platform donde se encuentra la tabla de BigQuery.
    dataset : str
        Nombre del dataset en BigQuery donde se encuentra la tabla "archivos_procesados".
    archivos_nuevos : list
        Lista de nombres de archivos nuevos a registrar.
    """
    
    # Validación de parámetros
    if not project_id or not dataset or not archivos_nuevos:
        raise ValueError("Los parámetros project_id, dataset y archivos_nuevos no pueden estar vacíos.")
    
    try:
        client = bigquery.Client()
        table_id = f"{project_id}.{dataset}.archivos_procesados"
        
        # Preparar las filas a insertar
        rows_to_insert = [
            {"nombre_archivo": archivo, "fecha_carga": datetime.now().isoformat()} 
            for archivo in archivos_nuevos
        ]
        logging.info(f"Registrando archivos en BigQuery: {archivos_nuevos}")
        
        # Insertar las filas
        errors = client.insert_rows_json(table_id, rows_to_insert)
        
        if errors:
            logging.error(f"Error al insertar los archivos procesados: {errors}")
        else:
            logging.info(f"Archivos nuevos registrados exitosamente: {archivos_nuevos}")
    
    except Exception as e:
        logging.error(f"Error en registrar_archivos_en_bq: {e}")
    finally:
        client.close()  # Cierra el cliente de BigQuery si no se utilizará más



def obtener_archivos_nuevos_version_premium(bucket_name: str, prefix: str, project_id: str, dataset: str) -> list:
    """
    Detecta archivos nuevos en un bucket de Google Cloud Storage comparando con los archivos ya registrados en BigQuery.
    """
    client = bigquery.Client()
    storage_client = storage.Client()
    table_id = f"{project_id}.{dataset}.archivos_procesados"

    # Consulta para obtener la lista de archivos ya procesados en BigQuery
    print("Consultando archivos procesados en BigQuery...")
    query = f"SELECT nombre_archivo FROM `{table_id}`"
    query_job = client.query(query)
    archivos_procesados = {row.nombre_archivo for row in query_job}

    # Lista de archivos actuales en el bucket de Cloud Storage con el prefijo especificado
    print(f"Listando archivos en el bucket {bucket_name} con prefijo '{prefix}'...")
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
    archivos = [blob.name for blob in blobs if not blob.name.endswith('/')]  # Filtrar directorios

    # Filtra los archivos que aún no han sido procesados
    archivos_nuevos = [archivo for archivo in archivos if archivo not in archivos_procesados]

    print(f"Archivos nuevos detectados: {archivos_nuevos}")
    return archivos_nuevos
