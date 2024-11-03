from google.cloud import bigquery, storage  
from datetime import datetime
import logging

def obtener_archivos_nuevos(bucket_name: str, prefix: str, project_id: str, dataset: str) -> str:
    """
    Detecta archivos nuevos en un bucket de Google Cloud Storage y registra los archivos procesados en BigQuery.
    Esta función evita reprocesar archivos ya registrados en la tabla de BigQuery.

    Parámetros:
    -----------
    bucket_name : str
        Nombre del bucket en Google Cloud Storage donde están los archivos JSON.
    prefix : str
        Prefijo de la ruta en el bucket para filtrar los archivos (por ejemplo, "datasets/google/sitios/").
    project_id : str
        ID del proyecto en Google Cloud Platform donde se encuentra la tabla de BigQuery.
    dataset : str
        Nombre del dataset en BigQuery donde se encuentra la tabla "archivos_procesados".

    Retorna:
    --------
    str
        Nombre del primer archivo nuevo que no se ha procesado previamente o None si no hay archivos nuevos.
    """
    
    # Inicializa el cliente de BigQuery
    client = bigquery.Client()
    
    # Inicializa el cliente de Cloud Storage
    storage_client = storage.Client()
    
    # Define el ID de la tabla de BigQuery en formato "project.dataset.table"
    table_id = f"{project_id}.{dataset}.archivos_procesados"

    # Consulta para obtener la lista de archivos ya procesados en BigQuery
    query = f"SELECT nombre_archivo FROM `{table_id}`"
    query_job = client.query(query)  # Ejecuta la consulta
    archivos_procesados = {row.nombre_archivo for row in query_job}  # Convierte los resultados en un conjunto para búsqueda rápida

    # Lista de archivos actuales en el bucket de Cloud Storage con el prefijo especificado
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
    archivos = [
        blob.name for blob in blobs 
        if blob.name != prefix  # Omitir el archivo `g_sitios` (la carpeta)
    ]

    # Filtra los archivos que aún no han sido procesados
    archivos_nuevos = [archivo for archivo in archivos if archivo not in archivos_procesados]

    # Toma solo el primer archivo nuevo, si existe
    archivo_a_procesar = archivos_nuevos[0] if archivos_nuevos else None

    # Retorna el nombre del primer archivo nuevo detectado o None
    return archivo_a_procesar

##############################################################################

def registrar_archivos_en_bq(project_id: str, dataset: str, archivo_nuevo: str) -> None:
    """
    Registra un archivo nuevo en la tabla 'archivos_procesados' de BigQuery.
    
    Parámetros:
    -----------
    project_id : str
        ID del proyecto en Google Cloud Platform donde se encuentra la tabla de BigQuery.
    dataset : str
        Nombre del dataset en BigQuery donde se encuentra la tabla "archivos_procesados".
    archivo_nuevo : str
        Nombre del archivo nuevo a registrar.
    """
    
    # Validación de parámetros
    if not project_id or not dataset or not archivo_nuevo:
        raise ValueError("Los parámetros project_id, dataset y archivo_nuevo no pueden estar vacíos.")
    
    try:
        client = bigquery.Client()
        table_id = f"{project_id}.{dataset}.archivos_procesados"
        
        # Preparar la fila a insertar
        row_to_insert = {"nombre_archivo": archivo_nuevo, "fecha_carga": datetime.now().isoformat()}
        logging.info(f"Registrando archivo en BigQuery: {archivo_nuevo}")
        
        # Insertar la fila
        errors = client.insert_rows_json(table_id, [row_to_insert])
        
        if errors:
            logging.error(f"Error al insertar el archivo procesado: {errors}")
        else:
            logging.info(f"Archivo nuevo registrado exitosamente: {archivo_nuevo}")
    
    except Exception as e:
        logging.error(f"Error en registrar_archivos_en_bq: {e}")
    finally:
        client.close()  # Cierra el cliente de BigQuery si no se utilizará más

###################################################################################

def obtener_archivos_nuevos_y_registrar(bucket_name: str, prefix: str, project_id: str, dataset: str, temp_table: str) -> None:
    """
    Detecta archivos nuevos en un bucket de Google Cloud Storage y los registra en una tabla temporal en BigQuery.
    """
    client = bigquery.Client()
    storage_client = storage.Client()
    table_id = f"{project_id}.{dataset}.{temp_table}"
    
    # Obtener lista de archivos procesados desde la tabla en BigQuery
    query = f"SELECT nombre_archivo FROM `{table_id}`"
    query_job = client.query(query)
    archivos_procesados = {row.nombre_archivo for row in query_job}

    # Obtener lista de archivos actuales en el bucket de Cloud Storage
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
    archivos_nuevos = [blob.name for blob in blobs if blob.name not in archivos_procesados and not blob.name.endswith('/')]

    # Registrar archivos nuevos en la tabla temporal
    if archivos_nuevos:
        rows_to_insert = [{"nombre_archivo": archivo, "fecha_carga": datetime.now().isoformat()} for archivo in archivos_nuevos]
        client.insert_rows_json(table_id, rows_to_insert)
        print(f"Archivos nuevos registrados exitosamente: {archivos_nuevos}")
    else:
        print("No se encontraron archivos nuevos para registrar.")
