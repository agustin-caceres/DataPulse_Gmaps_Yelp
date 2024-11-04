from google.cloud import storage
import json

################################################################################################

def dict_to_list(diccionario: dict) -> list:
    """
    Convierte el diccionario de la columna `MISC` en una lista de strings
    "key: value" excluyendo pares donde el valor es nulo o una lista vacía.

    Args:
        diccionario (dict): Diccionario recibido.

    Returns:
        list: Lista con los pares "clave: valor" no nulos ni vacíos.
    """
    return [f"{key}: {', '.join(value)}" if isinstance(value, list) else f"{key}: {value}"
            for key, value in diccionario.items() if value]
    
################################################################################################

def desanidar_misc(bucket_name: str, archivo: str, bucket_procesado: str) -> None:
    """
    Toma un archivo JSON de Google Cloud Storage, extrae y desanida la columna 'MISC' 
    y guarda el resultado en un bucket de Google Cloud Storage en formato NDJSON.

    Args:
    -------
    bucket_name : str
        Nombre del bucket en Google Cloud Storage donde se encuentra el archivo original.
    archivo : str
        Nombre del archivo JSON que contiene la columna 'MISC'.
    bucket_procesado : str
        Nombre del bucket de Google Cloud Storage donde se guardará el archivo desanidado.
    """
    # Inicializa el cliente de Cloud Storage
    storage_client = storage.Client()

    # Define los buckets de origen y destino
    bucket_entrada = storage_client.bucket(bucket_name)
    bucket_salida = storage_client.bucket(bucket_procesado)
    
    # Lee el archivo JSON desde el bucket de entrada
    blob = bucket_entrada.blob(archivo)
    contenido = blob.download_as_text()

    # Lista para almacenar los registros desanidados
    registros_desanidados = []

    # Procesa cada línea del archivo como un objeto JSON
    for linea in contenido.splitlines():
        try:
            contenido_json = json.loads(linea)
        except json.JSONDecodeError as e:
            print(f"Error de decodificación JSON en la línea: {e}")
            continue

        # Extrae gmap_id y MISC
        gmap_id = contenido_json.get('gmap_id')
        misc = contenido_json.get('MISC')

        if gmap_id is None or misc is None:
            print(f"Registro sin 'gmap_id' o sin 'MISC' en archivo {archivo}. Se omite.")
            continue

        # Convierte el diccionario `MISC` en una lista usando `dict_to_list`
        lista_misc = dict_to_list(misc)

        # Genera el nuevo formato desanidado
        for item in lista_misc:
            registros_desanidados.append({"gmap_id": gmap_id, "misc": item})

    # Define el nombre del archivo desanidado
    nombre_archivo_procesado = archivo.replace('.json', '_procesado.ndjson')
    blob_salida = bucket_salida.blob(nombre_archivo_procesado)

    # Guarda los registros desanidados en formato NDJSON en el bucket de procesados
    with blob_salida.open("w") as f:
        for registro in registros_desanidados:
            f.write(json.dumps(registro) + "\n")
    
    print(f"Archivo desanidado guardado en {bucket_procesado} como {nombre_archivo_procesado}.")

#########################################################################################

def procesar_archivo(bucket_entrada: str, bucket_procesado: str, archivos: list) -> None:
    """
    Procesa todos los archivos JSON, desanida la columna 'MISC' y los guarda en el bucket procesado.

    Args:
        bucket_entrada (str): Nombre del bucket de entrada donde se encuentran los archivos.
        bucket_procesado (str): Nombre del bucket donde se guardarán los archivos procesados.
        archivos (list): Lista de archivos a procesar.
    """
    for archivo in archivos:
        # Asegúrate de que el nombre del archivo es correcto
        desanidar_misc(bucket_name=bucket_entrada, archivo=archivo, bucket_procesado=bucket_procesado)
