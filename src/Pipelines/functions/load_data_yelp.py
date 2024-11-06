from google.cloud import bigquery
import pandas as pd
import logging
from datetime import datetime

# Configuración del logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Función para Registrar un Archivo en la Tabla de Control
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
    
    rows_to_insert = [{
        "nombre_archivo": nombre_archivo,
        "fecha_procesamiento": bigquery.Timestamp(datetime.now())
    }]
    
    client.insert_rows_json(table_id, rows_to_insert)
    logger.info(f"Archivo '{nombre_archivo}' registrado exitosamente como procesado en la tabla de control.")


# Función para Verificar si un Archivo ya fue Procesado
def archivo_procesado(project_id: str, dataset: str, nombre_archivo: str) -> bool:
    """
    Verifica si un archivo ya fue procesado consultando la tabla de control en BigQuery.
    
    Args:
        project_id (str): ID del proyecto de GCP.
        dataset (str): Nombre del dataset en BigQuery.
        nombre_archivo (str): Nombre del archivo a verificar.
    
    Returns:
        bool: True si el archivo ya fue procesado, False en caso contrario.
    """
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset}.archivos_procesados"
    logger.info(f"Verificando si el archivo '{nombre_archivo}' ya fue procesado en '{table_id}'.")

    query = f"""
    SELECT COUNT(1) AS procesado
    FROM `{project_id}.{dataset}.archivos_procesados`
    WHERE nombre_archivo = '{nombre_archivo}'
    """
    
    query_job = client.query(query)
    result = query_job.result()
    
    archivo_ya_procesado = result.total_rows > 0
    if archivo_ya_procesado:
        logger.info(f"Archivo '{nombre_archivo}' ya ha sido procesado previamente.")
    else:
        logger.info(f"Archivo '{nombre_archivo}' no ha sido procesado aún.")
    
    return archivo_ya_procesado


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
