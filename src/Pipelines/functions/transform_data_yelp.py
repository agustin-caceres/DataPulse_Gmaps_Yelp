import pandas as pd
from google.cloud import bigquery

def transformar_checkin(project_id: str, dataset: str, temp_table: str, final_table: str) -> None:
    """
    General la consulta SQL y realiza la transformación de los datos en la tabla temporal `checkin_temp` de BigQuery,
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
        CAST(date AS DATETIME) AS date
    FROM `{project_id}.{dataset}.{temp_table}`
    WHERE date IS NOT NULL
    """
    
    # Ejecuta la consulta
    client.query(query).result()
    print(f"Transformación completada y datos cargados en `{final_table}`.")

