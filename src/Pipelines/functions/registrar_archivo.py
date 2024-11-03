from google.cloud import bigquery
from google.cloud import storage  
from datetime import datetime


def registrar_archivos_procesados(bucket_name: str, prefix: str, project_id: str, dataset: str) -> str:
    """
    Detecta archivos nuevos en un bucket de Google Cloud Storage y registra los archivos procesados en BigQuery.
    Esta función evita reprocesar archivos ya registrados en la tabla de BigQuery.

    Parámetros:
    -----------
    bucket_name : str
        Nombre del bucket en Google Cloud Storage donde están los archivos JSON.
    prefix : str
        Prefijo de la ruta en el bucket para filtrar los archivos (por ejemplo, "datasets/google/sitios/").
    project_id : str
        ID del proyecto en Google Cloud Platform donde se encuentra la tabla de BigQuery.
    dataset : str
        Nombre del dataset en BigQuery donde se encuentra la tabla "archivos_procesados".

    Retorna:
    --------
    str
        Nombre del primer archivo nuevo que no se ha procesado previamente o None si no hay archivos nuevos.
    """

    # Inicializa el cliente de BigQuery
    client = bigquery.Client()
    
    # Inicializa el cliente de Cloud Storage
    storage_client = storage.Client() 
    
    # Define el ID de la tabla de BigQuery en formato "project.dataset.table"
    table_id = f"{project_id}.{dataset}.archivos_procesados"

    # Consulta para obtener la lista de archivos ya procesados en BigQuery
    query = f"SELECT nombre_archivo FROM `{table_id}`"
    query_job = client.query(query)  # Ejecuta la consulta
    archivos_procesados = {row.nombre_archivo for row in query_job}  # Convierte los resultados en un conjunto para búsqueda rápida

    # Lista de archivos actuales en el bucket de Cloud Storage con el prefijo especificado
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
    archivos = [blob.name for blob in blobs]  # Obtiene el nombre de cada archivo en el bucket

    # Filtra los archivos que aún no han sido procesados
    archivos_nuevos = [archivo for archivo in archivos if archivo not in archivos_procesados]

    # Toma solo el primer archivo nuevo, si existe
    archivo_a_procesar = archivos_nuevos[0] if archivos_nuevos else None

    # Si hay archivos nuevos, los registra en BigQuery
    if archivo_a_procesar:
        rows_to_insert = [{
            "nombre_archivo": archivo_a_procesar,
            "fecha_carga": datetime.now().isoformat()
        }]
        
        errors = client.insert_rows_json(table_id, rows_to_insert)
        if errors:
            print(f"Error al insertar el archivo procesado: {errors}")
        else:
            print(f"Archivo nuevo registrado exitosamente: {archivo_a_procesar}")
    else:
        print("No se encontraron archivos nuevos para procesar.")

    # Retorna el nombre del primer archivo nuevo detectado o None
    return archivo_a_procesar
