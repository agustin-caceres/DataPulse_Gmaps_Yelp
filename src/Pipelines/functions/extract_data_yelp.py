import pandas as pd
from google.cloud import storage
import io
import logging
from functions.transform_data_yelp import aplicar_transformacion

# Configuración del logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def cargar_archivo_gcs_a_dataframe(bucket_name: str, file_path: str) -> pd.DataFrame:
    """
    Extrae un archivo desde Google Cloud Storage y lo convierte en un DataFrame.
    Aplica la transformación específica según el nombre del archivo.

    Args:
        bucket_name (str): Nombre del bucket en GCS.
        file_path (str): Ruta del archivo en el bucket.

    Returns:
        pd.DataFrame: DataFrame con los datos extraídos y transformados del archivo.
    """
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_path)
    
    logger.info(f"Iniciando descarga del archivo '{file_path}' desde el bucket '{bucket_name}'.")
    
    # Descarga y procesamiento del archivo en función de su tipo
    if file_path.endswith('.json'):
        data = blob.download_as_text()
        df = pd.read_json(io.StringIO(data), lines=True)
        logger.info(f"Archivo JSON '{file_path}' cargado exitosamente en un DataFrame.")
        
    elif file_path.endswith('.parquet'):
        df = pd.read_parquet(io.BytesIO(blob.download_as_bytes()))
        logger.info(f"Archivo Parquet '{file_path}' cargado exitosamente en un DataFrame.")
        
    elif file_path.endswith('.pkl'):
        df = pd.read_pickle(io.BytesIO(blob.download_as_bytes()))
        logger.info(f"Archivo Pickle '{file_path}' cargado exitosamente en un DataFrame.")
        
    else:
        logger.error(f"Formato de archivo no soportado: {file_path}")
        raise ValueError(f"Formato de archivo no soportado: {file_path}")

    # Aplicación de la transformación específica si existe en el diccionario
    df = aplicar_transformacion(file_path, df)
    logger.info(f"Transformación específica aplicada al archivo '{file_path}'.")

    return df
