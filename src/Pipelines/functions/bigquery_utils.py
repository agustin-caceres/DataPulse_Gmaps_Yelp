from google.cloud import bigquery
import pandas as pd
import io

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
    try:
        client = bigquery.Client(project=project_id)
        table_id = f"{project_id}.{dataset}.{temp_table}"
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table, exists_ok=True)
        print(f"Tabla temporal '{table_id}' creada o ya existente.")

    except Exception as e:
        print(f"Error al crear la tabla temporal {temp_table}: {e}")

#######################################################################################

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
    try:
        if df.empty:
            print("El DataFrame está vacío. No se cargarán datos en BigQuery.")
            return

        client = bigquery.Client(project=project_id)
        table_id = f"{project_id}.{dataset}.{table_name}"
        job = client.load_table_from_dataframe(df, table_id)
        job.result()  # Espera a que la carga se complete
        print(f"Datos cargados en la tabla {table_id}")

    except Exception as e:
        print(f"Error al cargar los datos en la tabla {table_name}: {e}")

#######################################################################################

import pandas as pd
from google.cloud import bigquery, storage
import io

def cargar_archivo_gcs_a_bigquery(bucket_name: str, file_path: str, project_id: str, dataset: str, table_name: str) -> None:
    """
    Extrae un archivo desde Google Cloud Storage, lo procesa y lo carga en BigQuery.

    Args:
        bucket_name (str): Nombre del bucket en GCS.
        file_path (str): Ruta del archivo en el bucket.
        project_id (str): ID del proyecto de GCP.
        dataset (str): Nombre del dataset en BigQuery.
        table_name (str): Nombre de la tabla de destino en BigQuery.

    Returns:
        None
    """
    try:
        # Conexión a GCS y descarga del archivo
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(file_path)
        data = blob.download_as_text()

        # Determina el formato del archivo y carga en DataFrame
        if file_path.endswith('.json'):
            df = pd.read_json(io.StringIO(data), lines=True)
        elif file_path.endswith('.parquet'):
            df = pd.read_parquet(io.BytesIO(blob.download_as_bytes()))
        elif file_path.endswith('.pkl'):
            df = pd.read_pickle(io.BytesIO(blob.download_as_bytes()))
        else:
            print("Formato de archivo no soportado.")
            return

        # Aplica transformación específica si existe en el diccionario
        archivo_nombre = file_path.split('/')[-1]
        if archivo_nombre in transformaciones:
            df = transformaciones[archivo_nombre](df)

        # Cargar el DataFrame en BigQuery
        client_bq = bigquery.Client(project=project_id)
        table_id = f"{project_id}.{dataset}.{table_name}"

        if df.empty:
            print("El DataFrame está vacío. No se cargarán datos en BigQuery.")
            return

        job = client_bq.load_table_from_dataframe(df, table_id)
        job.result()  # Espera a que la carga se complete
        print(f"Datos de {file_path} cargados en la tabla {table_id}")

    except Exception as e:
        print(f"Error al procesar y cargar el archivo {file_path}: {e}")

#######################################################################################

def transformar_checkin(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma el DataFrame de `checkin.json` separando el campo `date` en filas individuales.

    Args:
        df (pd.DataFrame): DataFrame original de `checkin.json`.

    Returns:
        pd.DataFrame: DataFrame transformado con fechas separadas en filas individuales.
    """
    return df.assign(date=df['date'].str.split(', ')).explode('date').reset_index(drop=True)


# Diccionario de transformaciones basado en el nombre del archivo
transformaciones = {
    'checkin.json': transformar_checkin,
    # 'otro_archivo.json': transformar_otro_archivo,  # Ejemplo de otro archivo con transformación específica
}
