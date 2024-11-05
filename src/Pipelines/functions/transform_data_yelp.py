import pandas as pd
from google.cloud import bigquery

def pre_transform_checkin(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma el DataFrame de `checkin.json` separando el campo `date` en filas individuales,
    manteniendo el formato completo de fecha.

    Args:
        df (pd.DataFrame): DataFrame original de `checkin.json`.

    Returns:
        pd.DataFrame: DataFrame transformado con fechas separadas en filas individuales.
    """
    # Validación de existencia del campo 'date'
    if 'date' not in df.columns:
        raise ValueError("El DataFrame no contiene el campo 'date'")

    # Separar el campo 'date' en múltiples fechas sin romper el formato
    df = df.assign(date=df['date'].str.split(', ')).explode('date').reset_index(drop=True)

    # Convertir el campo 'date' en formato datetime
    df['date'] = pd.to_datetime(df['date'], errors='coerce', format='%Y-%m-%d %H:%M:%S')

    # Filtrar filas donde 'date' no es nulo (si alguna fecha no se pudo convertir, se eliminará)
    df = df.dropna(subset=['date'])
    return df


# Diccionario de transformaciones basado en el nombre del archivo (pre-carga en BigQuery)
transformaciones = {
    'checkin.json': pre_transform_checkin,
    # 'otro_archivo.json': transformar_otro_archivo,  # Ejemplo de otro archivo con transformación específica
}

#######################################################################################

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
        CAST(date AS DATETIME) AS date  -- Convierte 'date' a DATETIME
    FROM `{project_id}.{dataset}.{temp_table}`
    WHERE date IS NOT NULL             -- Elimina registros donde 'date' es nulo
    """
    
    # Ejecuta la consulta
    client.query(query).result()
    print(f"Transformación completada y datos cargados en `{final_table}`.")
