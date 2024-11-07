from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import logging

###################################################

def desanidar_categorias(bucket_name: str, archivo: str, project_id: str, dataset: str) -> None:
    """
    Toma un archivo JSON de Google Cloud Storage, extrae los valores de 'category' 
    y los guarda desanidados en BigQuery.

    Args:
    -------
    bucket_name : str
        Nombre del bucket en Google Cloud Storage.
    archivo : str
        Nombre del archivo JSON que contiene la columna 'category'.
    project_id : str
        ID del proyecto en Google Cloud Platform.
    dataset : str
        Nombre del dataset en BigQuery donde se guardará la tabla 'category'.
    """
    
    # Inicializa el cliente de BigQuery y el cliente de Cloud Storage
    client = bigquery.Client()
    storage_client = storage.Client()

    # Define el ID de la tabla de destino
    table_id = f"{project_id}.{dataset}.g_categorias"

    # Lee el archivo JSON desde Cloud Storage
    blob = storage_client.bucket(bucket_name).blob(archivo)
    contenido = blob.download_as_text()

    # Carga el archivo JSON en un DataFrame de Pandas
    df = pd.read_json(contenido, lines=True)

    # Filtra registros sin información en 'relative_results' o 'gmap_id'
    df = df[df['category'].notna() & df['gmap_id'].notna()]

    # Expande la columna 'relative_results' usando explode
    df_expanded = df[['gmap_id', 'category']].explode('category').dropna()

    # Carga el DataFrame resultante a BigQuery y verifica si la tabla ya existe y tiene datos
    table = client.get_table(table_id)
    
    if table.num_rows == 0:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    else:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

    client.load_table_from_dataframe(df_expanded[['gmap_id', 'category']], table_id, job_config=job_config).result()
    logging.info(f"Datos del archivo {archivo} cargados exitosamente en la tabla 'category' de BigQuery.")