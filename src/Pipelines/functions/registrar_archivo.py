from google.cloud import bigquery
from google.cloud import storage  
from datetime import datetime

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
