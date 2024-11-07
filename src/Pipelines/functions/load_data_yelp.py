from google.cloud import bigquery
import pandas as pd
import logging
from datetime import datetime
from google.cloud import storage


# Configuración del logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def registrar_archivo_procesado(project_id: str, dataset: str, nombre_archivo: str) -> None:
    """
    Registra un archivo como procesado en la tabla de control en BigQuery.
    
    Args:
        project_id (str): ID del proyecto de GCP.
        dataset (str): Nombre del dataset en BigQuery.
        nombre_archivo (str): Nombre del archivo procesado.
    """
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset}.archivos_procesados"
    
    logger.info(f"Registrando el archivo '{nombre_archivo}' en la tabla de control '{table_id}'.")

    # Inserta la fecha y hora actual en formato compatible con BigQuery
    rows_to_insert = [{
        "nombre_archivo": nombre_archivo,
        "fecha_carga": datetime.now().isoformat()  # Formato compatible con BigQuery
    }]
    
    client.insert_rows_json(table_id, rows_to_insert)
    logger.info(f"Archivo '{nombre_archivo}' registrado exitosamente como procesado en la tabla de control.")


# Función para Verificar si un Archivo ya fue Procesado
def archivo_procesado(project_id: str, dataset: str, nombre_archivo: str, fecha_actualizacion: datetime) -> bool:
    """
    Verifica si un archivo ha sido procesado recientemente consultando la tabla de control en BigQuery.
    
    Args:
        project_id (str): ID del proyecto de GCP.
        dataset (str): Nombre del dataset en BigQuery.
        nombre_archivo (str): Nombre del archivo a verificar.
        fecha_actualizacion (datetime): Fecha de la última actualización del archivo en el bucket.
    
    Returns:
        bool: True si el archivo ya fue procesado recientemente, False en caso contrario.
    """
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset}.archivos_procesados"
    logger.info(f"Verificando si el archivo '{nombre_archivo}' ya fue procesado en '{table_id}'.")

    # Consulta SQL para obtener la última fecha de carga del archivo
    query = f"""
    SELECT MAX(fecha_carga) AS ultima_fecha_carga
    FROM `{table_id}`
    WHERE nombre_archivo = '{nombre_archivo}'
    """
    
    query_job = client.query(query)
    result = query_job.result()
    
    # Obtén la última fecha de carga registrada para el archivo
    ultima_fecha_carga = next(result).ultima_fecha_carga

    # Verifica si la última fecha de carga es mayor o igual a la fecha de actualización del archivo en el bucket
    if ultima_fecha_carga and ultima_fecha_carga >= fecha_actualizacion:
        logger.info(f"Archivo '{nombre_archivo}' ya ha sido procesado recientemente el {ultima_fecha_carga}.")
        return True

    logger.info(f"Archivo '{nombre_archivo}' no ha sido procesado recientemente o es una nueva actualización.")
    return False


def obtener_fecha_actualizacion(bucket_name: str, archivo_nombre: str) -> datetime:
    """
    Obtiene la fecha de actualización de un archivo en Google Cloud Storage.

    Args:
        bucket_name (str): Nombre del bucket en GCS.
        archivo_nombre (str): Nombre del archivo.

    Returns:
        datetime: Fecha de actualización del archivo, o None si el archivo no existe.
    """
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.get_blob(archivo_nombre)
    
    if blob:
        return blob.updated  # Devuelve la fecha de última modificación
    else:
        logger.warning(f"El archivo '{archivo_nombre}' no existe en el bucket '{bucket_name}'")
        return None


def crear_tabla_temporal(project_id: str, dataset: str, temp_table: str, schema: list) -> None:
    """
    Crea una tabla temporal en BigQuery con el esquema especificado.

    Args:
        project_id (str): ID del proyecto de GCP.
        dataset (str): Nombre del dataset en BigQuery.
        temp_table (str): Nombre de la tabla temporal a crear.
        schema (list): Lista de campos con el esquema de la tabla.

    Returns:
        None
    """
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset}.{temp_table}"
    table = bigquery.Table(table_id, schema=schema)
    
    client.create_table(table, exists_ok=True)
    logger.info(f"Tabla temporal '{table_id}' creada o ya existente.")


def cargar_dataframe_a_bigquery(df: pd.DataFrame, project_id: str, dataset: str, table_name: str) -> None:
    """
    Carga un DataFrame en una tabla específica de BigQuery.

    Args:
        df (pd.DataFrame): DataFrame a cargar en BigQuery.
        project_id (str): ID del proyecto de GCP.
        dataset (str): Nombre del dataset en BigQuery.
        table_name (str): Nombre de la tabla donde se cargará el DataFrame.

    Returns:
        None
    """
    if df.empty:
        logger.warning("El DataFrame está vacío. No se cargarán datos en BigQuery.")
        return

    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset}.{table_name}"
    
    logger.info(f"Iniciando carga de datos en la tabla '{table_id}'.")
    job = client.load_table_from_dataframe(df, table_id)
    job.result()  # Espera a que la carga se complete
    logger.info(f"Datos cargados exitosamente en la tabla '{table_id}'.")


def eliminar_tabla_temporal(project_id: str, dataset: str, table_name: str) -> None:
    """
    Elimina una tabla en BigQuery.

    Args:
        project_id (str): ID del proyecto de GCP.
        dataset (str): Nombre del dataset en BigQuery.
        table_name (str): Nombre de la tabla a eliminar.

    Returns:
        None
    """
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset}.{table_name}"
    
    client.delete_table(table_id, not_found_ok=True)
    logger.info(f"Tabla temporal '{table_id}' eliminada.")
