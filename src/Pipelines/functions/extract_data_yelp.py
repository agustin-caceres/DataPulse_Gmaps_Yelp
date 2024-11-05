import pandas as pd
from google.cloud import storage
import io

def extract_checkin_json(bucket_name: str, file_path: str) -> pd.DataFrame:
    """
    Extrae y transforma el archivo `checkin.json` desde Google Cloud Storage.

    Args:
        bucket_name (str): Nombre del bucket en GCS.
        file_path (str): Ruta del archivo JSON en el bucket.

    Returns:
        pd.DataFrame: DataFrame con los datos transformados de `checkin.json`, con cada fecha en una fila separada.
    """
    try:
        # Conexión a GCS y descarga del archivo JSON
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(file_path)
        data = blob.download_as_text()

        # Carga el JSON en un DataFrame
        df = pd.read_json(io.StringIO(data), lines=True)

        # Normalización del campo 'date' (separación de fechas en múltiples filas)
        df = df.assign(date=df['date'].str.split(', ')).explode('date').reset_index(drop=True)
        print("Archivo extraído y transformado exitosamente.")
        return df

    except Exception as e:
        print(f"Error en la extracción y transformación del archivo {file_path}: {e}")
        return pd.DataFrame()  # Retorna un DataFrame vacío en caso de error
