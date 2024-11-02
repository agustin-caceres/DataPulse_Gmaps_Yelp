from google.cloud import bigquery
from google.cloud import storage
import json

def dict_to_list(diccionario: dict) -> list:
    """
    Devuelve el Diccionario de la columna `MISC` y lo retorna
    como una lista de String "key: value" siempre y cuando el 
    valor de la pareja no sea nulo.

    Args:
        diccionario (dict): diccionario recibido

    Returns:
        list: Lista a devolver
    """
    return [f"{key}: {value}" for key, value in diccionario.items() if value is not None]

def desanidar_misc(bucket_name: str, archivo: str, project_id: str, dataset: str) -> None:
    """
    Toma un archivo JSON de Google Cloud Storage, extrae los valores de la columna 'MISC' 
    y los guarda desanidados en BigQuery.

    Parámetros:
    -----------
    bucket_name : str
        Nombre del bucket en Google Cloud Storage.
    archivo : str
        Nombre del archivo JSON que contiene la columna 'MISC'.
    project_id : str
        ID del proyecto en Google Cloud Platform donde se encuentra la tabla de destino.
    dataset : str
        Nombre del dataset en BigQuery donde se encuentra la tabla de destino 'miscelaneos'.
    """

    # Inicializa el cliente de BigQuery
    client = bigquery.Client()
    
    # Define el ID de la tabla de destino
    table_id = f"{project_id}.{dataset}.miscelaneos"

    # Inicializa el cliente de Cloud Storage
    storage_client = storage.Client()
    
    # Obtiene el blob (archivo) desde el bucket
    blob = storage_client.bucket(bucket_name).blob(archivo)
    
    # Lee el contenido del archivo JSON
    contenido_json = json.loads(blob.download_as_text())
    
    # Extrae el gmap_id del JSON
    gmap_id = contenido_json.get('gmap_id', None)  # Cambia 'gmap_id' según la estructura de tu JSON
    
    if gmap_id is None:
        print(f"No se encontró el gmap_id en el archivo {archivo}.")
        return

    # Extrae y desanida los datos de MISC
    misc = contenido_json.get('MISC', {})
    lista_misc = dict_to_list(misc)  # Convierte el diccionario en una lista

    # Prepara los datos desanidados para insertar en BigQuery
    rows_to_insert = []
    
    for item in lista_misc:
        rows_to_insert.append({
            "gmap_id": gmap_id,
            "misc": item  # Almacena el valor desanidado
        })

    # Inserta los datos desanidados en BigQuery
    if rows_to_insert:
        errors = client.insert_rows_json(table_id, rows_to_insert)
        if errors:
            print(f"Error al insertar los datos desanidados: {errors}")
        else:
            print(f"Datos desanidados del archivo {archivo} cargados exitosamente en BigQuery.")
    else:
        print(f"No se encontraron datos para insertar desde el archivo {archivo}.")

# Ejemplo de uso
# desanidar_misc('tu_nombre_de_bucket', 'nombre_del_archivo.json', 'tu_project_id', 'tu_dataset')
