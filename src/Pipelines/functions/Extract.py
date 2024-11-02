from google.cloud import storage
import json

def extract_data(bucket_name: str, nombre_archivo: str) -> dict:
    """
    Extrae y desanida los datos de un archivo JSON en Cloud Storage, preparándolos para la fase de transformación.

    Parámetros:
    -----------
    bucket_name : str
        Nombre del bucket en Google Cloud Storage donde se encuentra el archivo.
    nombre_archivo : str
        Nombre del archivo JSON a extraer.

    Retorna:
    --------
    dict
        Datos extraídos y desanidados del archivo JSON.
    """

    # Inicializa el cliente de Cloud Storage
    storage_client = storage.Client()
    
    # Obtiene el bucket y el blob (archivo JSON)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(nombre_archivo)
    
    # Descarga el contenido del archivo JSON como texto y lo carga como diccionario
    json_data = json.loads(blob.download_as_text())
    
    # Desanidar datos si es necesario
    processed_data = {}
    for key, value in json_data.items():
        if isinstance(value, list):
            # Si el valor es una lista de listas (como "hours"), conviértelo en un string unificado
            if all(isinstance(i, list) for i in value):
                processed_data[key] = "; ".join([f"{day}: {time}" for day, time in value])
            else:
                # Si es una lista simple, únelos en una cadena separada por comas
                processed_data[key] = ", ".join(value)
        elif isinstance(value, dict):
            # Si el valor es un diccionario anidado (como "MISC"), conviértelo en un string unificado
            processed_data[key] = "; ".join([f"{sub_key}: {', '.join(sub_value)}" 
                                             for sub_key, sub_value in value.items()])
        else:
            # Si es un valor simple, lo asignamos directamente
            processed_data[key] = value

    return processed_data
