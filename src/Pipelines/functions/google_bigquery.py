from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import logging
from datetime import datetime

################################################################

def crear_tablas_bigquery(project_id: str, dataset: str) -> None:
    """
    Crea múltiples tablas en el dataset de BigQuery si no existen.
    
    Args:
    -------
    project_id : str
        ID del proyecto en Google Cloud Platform.
    dataset : str
        Nombre del dataset en BigQuery.
    """
    client = bigquery.Client()

    # Diccionario con las definiciones de tablas: nombre de la tabla y consulta SQL de creación
    tablas = {
        "miscelaneos": f"""
            CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.miscelaneos` (
                gmap_id STRING,
                misc STRING
            )
        """,
        "g_relative_results": f"""
            CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.g_relative_results` (
                gmap_id STRING,
                relative_results STRING
            )
        """,
        "g_categorias": f"""
            CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.g_categorias` (
                gmap_id STRING,
                categoria STRING
            )
        """,
        "g_horarios": f"""
            CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.g_horarios` (
                gmap_id STRING,
                monday STRING,
                tuesday STRING,
                wednesday STRING,
                thursday STRING,
                friday STRING,
                saturday STRING,
                sunday STRING
            )
        """,
        "g_address": f"""
            CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.g_address` (
                gmap_id STRING,
                address STRING,
                latitude FLOAT,
                longitude FLOAT,
                direccion STRING,
                ciudad STRING,
                cod_postal STRING,
                estado STRING,
                id_estado INT
            )
        """
    }
    
    # Ejecuta la consulta de creación para cada tabla
    for nombre_tabla, create_query in tablas.items():
        client.query(create_query).result()
        logging.info(f"Tabla '{nombre_tabla}' creada o ya existente.")
        
##############################################################################################

def eliminar_tablas_temporales(project_id: str, dataset: str) -> None:
    """
    Elimina las tablas temporales en BigQuery.
    
    Args:
    -------
    project_id : str
        ID del proyecto en Google Cloud Platform.
    dataset : str
        Nombre del dataset en BigQuery.
    """
    client = bigquery.Client()

    # Lista de tablas temporales a eliminar
    tablas_temporales = [
        f"{project_id}.{dataset}.temp_miscelaneos",
        f"{project_id}.{dataset}.miscelaneos"
    ]
    
    # Bucle para eliminar cada tabla temporal
    for table_id in tablas_temporales:
        drop_query = f"DROP TABLE IF EXISTS `{table_id}`"
        client.query(drop_query).result()
        logging.info(f"Tabla '{table_id}' eliminada con éxito.")

###################################################################################################

def detectar_archivos_nuevos(bucket_name: str, prefix: str, project_id: str, dataset: str) -> str:
    '''
    Detecta archivos nuevos en un bucket de Google Cloud Storage sin registrarlos en BigQuery.
    
    Retorna:
    --------
    str
        Nombre del primer archivo nuevo que no se ha procesado previamente o None si no hay archivos nuevos.
    '''
    storage_client = storage.Client()
    client = bigquery.Client()
    table_id = f"{project_id}.{dataset}.archivos_procesados"

    # Obtener archivos ya procesados
    query = f"SELECT nombre_archivo FROM `{table_id}`"
    archivos_procesados = {row.nombre_archivo for row in client.query(query)}

    # Listar archivos en el bucket que aún no han sido procesados
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
    archivos_nuevos = [blob.name for blob in blobs if blob.name not in archivos_procesados]

    return archivos_nuevos[0] if archivos_nuevos else None

###################################################################################################

def registrar_archivo_exitoso(archivo: str, project_id: str, dataset: str) -> None:
    '''
    Registra en BigQuery el archivo procesado después de una carga exitosa.
    
    Parámetros:
    -----------
    archivo : str
        Nombre del archivo procesado.
    '''
    client = bigquery.Client()
    table_id = f"{project_id}.{dataset}.archivos_procesados"

    rows_to_insert = [{
        "nombre_archivo": archivo,
        "fecha_carga": datetime.now().isoformat()
        }
    ]
    client.insert_rows_json(table_id, rows_to_insert)
    print(f"Archivo registrado exitosamente: {archivo}")