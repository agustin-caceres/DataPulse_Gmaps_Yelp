from google.cloud import bigquery
from google.cloud import storage  
from datetime import datetime

def registrar_archivos_procesados(bucket_name: str, prefix: str, project_id: str, dataset: str) -> None:
    """
    Registra el nombre del archivo en una tabla de BigQuery, que controla la fecha del procesamiento y el nombre para evitar duplicidades.

    Args:
        bucket_name (str): El nombre del bucket de Google Cloud Storage.
        prefix (str): ruta y nombre de la carpeta en el bucket.
        project_id (str): El ID del proyecto de Google Cloud.
        dataset (str): El nombre del dataset en BigQuery donde se registrarán los datos.

    Returns:
        None: Esta función no devuelve ningún valor. Imprime mensajes sobre el resultado 
        de la operación de inserción en BigQuery.
    """
    
    client = bigquery.Client()
    storage_client = storage.Client() 
    table_id = f"{project_id}.{dataset}.archivos_procesados"

    # Listar archivos en el bucket GCS con el prefijo especificado
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
    
    # Filtrar solo archivos que terminan en .json
    archivos = [blob.name for blob in blobs if blob.name.endswith('.json')] 

    rows_to_insert = []
    for nombre_archivo in archivos:
        rows_to_insert.append({
            "nombre_archivo": nombre_archivo,
            "fecha_carga": datetime.now().isoformat()
        })

    # Insertar los archivos en la tabla de BigQuery
    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        print(f"Error al insertar los archivos procesados: {errors}")
    else:
        print("Archivos registrados exitosamente.")  
