import logging
from google.cloud import storage
import json

# Configura el logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

###############################################################################################################

def procesar_archivos(bucket_entrada: str, bucket_procesado: str, archivos: list, prefix: str) -> None:
    """
    Procesa los archivos desde un bucket de entrada y los guarda en un bucket de salida.
    
    Args:
        bucket_entrada (str): El nombre del bucket de origen.
        bucket_procesado (str): El nombre del bucket donde se guardará el archivo procesado.
        archivos (list): Lista de nombres de archivos a procesar.
        prefix (str): Prefijo que indica la carpeta dentro del bucket.
    """
    # Filtrar archivos para incluir solo los archivos JSON y eliminar carpetas
    archivos_json = [archivo for archivo in archivos if archivo.endswith('.json') and not archivo.endswith('/')]

    logger.info(f"Archivos recibidos para procesar: {archivos}")
    logger.info(f"Archivos a procesar: {archivos_json}")

    if not archivos_json:
        logger.warning("No se encontraron archivos JSON o la lista no es válida.")
        return

    for archivo in archivos_json:
        # Procesa cada archivo individualmente
        logger.info(f"Procesando archivo: {archivo}")
        desanidar_misc(bucket_name=bucket_entrada, archivo=archivo, bucket_procesado=bucket_procesado, prefix=prefix)

################################################################################################

def desanidar_misc(bucket_name: str, archivo: str, bucket_procesado: str, prefix: str) -> None:
    """
    Toma un archivo JSON de Google Cloud Storage, extrae y desanida la columna 'MISC' 
    y guarda el resultado en un bucket de Google Cloud Storage en formato NDJSON.
    
    Args:
        bucket_name (str): El nombre del bucket de origen.
        archivo (str): El nombre del archivo en el bucket de origen.
        bucket_procesado (str): El nombre del bucket donde se guardará el archivo procesado.
        prefix (str): El prefijo del archivo dentro del bucket de origen.
    """
    # Inicializa el cliente de Cloud Storage
    storage_client = storage.Client()

    # Define los buckets de origen y destino
    bucket_entrada = storage_client.bucket(bucket_name)
    bucket_salida = storage_client.bucket(bucket_procesado)

    # Construye la ruta completa del archivo en el bucket de entrada
    archivo_completo = f"{prefix}/{archivo}" if prefix else archivo

    # Lee el archivo JSON desde el bucket de entrada
    blob = bucket_entrada.blob(archivo_completo)

    try:
        contenido = blob.download_as_text()
    except Exception as e:
        logger.error(f"Error al descargar el archivo {archivo_completo}: {e}")
        return

    # Lista para almacenar los registros desanidados
    registros_desanidados = []

    # Procesa cada línea del archivo como un objeto JSON
    for linea in contenido.splitlines():
        try:
            contenido_json = json.loads(linea)
        except json.JSONDecodeError as e:
            logger.warning(f"Error de decodificación JSON en la línea: {e}")
            continue

        # Extrae gmap_id y MISC
        gmap_id = contenido_json.get('gmap_id')
        misc = contenido_json.get('MISC')

        if gmap_id is None or misc is None:
            logger.warning(f"Registro sin 'gmap_id' o sin 'MISC' en archivo {archivo_completo}. Se omite.")
            continue

        if not isinstance(misc, dict):
            logger.warning(f"'MISC' no es un diccionario en el registro con gmap_id {gmap_id}. Se omite.")
            continue

        # Convierte el diccionario `MISC` en una lista usando `dict_to_list`
        lista_misc = dict_to_list(misc)  # Asegúrate de que dict_to_list esté definida

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
    
    logger.info(f"Archivo desanidado guardado en {bucket_procesado} como {nombre_archivo_procesado}.")
