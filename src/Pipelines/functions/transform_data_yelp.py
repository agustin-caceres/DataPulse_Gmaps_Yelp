import pandas as pd
import logging
from google.cloud import bigquery


# Configuración del logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def pre_transformar_checkin(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma el DataFrame de `checkin.json` separando el campo `date` en filas individuales
    y convirtiéndolo en un formato de fecha.

    Args:
        df (pd.DataFrame): DataFrame original de `checkin.json`.

    Returns:
        pd.DataFrame: DataFrame transformado con fechas separadas en filas individuales.
    """
    logger.info("Iniciando transformación del DataFrame de 'checkin.json'.")
    df = df.assign(date=df['date'].str.split(', ')).explode('date').reset_index(drop=True)
    df['date'] = pd.to_datetime(df['date'], errors='coerce', format='%Y-%m-%d %H:%M:%S')
    df = df.dropna(subset=['date'])
    logger.info("Transformación del DataFrame de 'checkin.json' completada.")
    return df

# Diccionario de transformaciones basado en el nombre del archivo
transformaciones = {
    'checkin.json': pre_transformar_checkin,
}

def aplicar_transformacion(file_path: str, df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica una transformación específica en función del nombre del archivo.

    Args:
        file_path (str): Ruta del archivo.
        df (pd.DataFrame): DataFrame a transformar.

    Returns:
        pd.DataFrame: DataFrame transformado.
    """
    archivo_nombre = file_path.split('/')[-1]
    if archivo_nombre in transformaciones:
        logger.info(f"Aplicando transformación para el archivo '{archivo_nombre}'.")
        return transformaciones[archivo_nombre](df)
    logger.info(f"No se requiere transformación para el archivo '{archivo_nombre}'.")
    return df


def transformar_checkin(project_id: str, dataset: str, temp_table: str, final_table: str) -> None:
    """
    Genera la consulta SQL y realiza la transformación de los datos en la tabla temporal `checkin_temp` de BigQuery,
    eliminando valores nulos en `date` y cambiando el tipo de dato de `TIMESTAMP` a `DATETIME`.
    
    Args:
        project_id (str): ID del proyecto de GCP.
        dataset (str): Nombre del dataset en BigQuery.
        temp_table (str): Nombre de la tabla temporal en BigQuery.
        final_table (str): Nombre de la tabla final en BigQuery.
    
    Returns:
        None
    """
    client = bigquery.Client(project=project_id)
    
    # Consulta de transformación en BigQuery
    query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset}.{final_table}` AS
    SELECT
        business_id,
        CAST(date AS DATETIME) AS date              -- Convierte 'date' a DATETIME
    FROM `{project_id}.{dataset}.{temp_table}`
    WHERE date IS NOT NULL                          -- Elimina registros donde 'date' es nulo
    """
    
    # Ejecuta la consulta
    client.query(query).result()
    logger.info(f"Transformación completada y datos cargados en `{final_table}`.")
