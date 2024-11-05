import pandas as pd
from google.cloud import storage
import io
from functions.transform_data_yelp import aplicar_transformacion

def cargar_archivo_gcs_a_dataframe(bucket_name: str, file_path: str) -> pd.DataFrame:
    """
    Extrae un archivo desde Google Cloud Storage y lo convierte en un DataFrame.

    Args:
        bucket_name (str): Nombre del bucket en GCS.
        file_path (str): Ruta del archivo en el bucket.

    Returns:
        pd.DataFrame: DataFrame con los datos extraídos del archivo.
    """
    try:
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(file_path)
        data = blob.download_as_text()

        # Determina el formato del archivo y carga en DataFrame
        if file_path.endswith('.json'):
            df = pd.read_json(io.StringIO(data), lines=True)
        elif file_path.endswith('.parquet'):
            df = pd.read_parquet(io.BytesIO(blob.download_as_bytes()))
        elif file_path.endswith('.pkl'):
            df = pd.read_pickle(io.BytesIO(blob.download_as_bytes()))
        else:
            print("Formato de archivo no soportado.")
            return pd.DataFrame()

        # Aplica transformación específica si existe en el diccionario
        df = aplicar_transformacion(file_path, df)
        return df

    except Exception as e:
        print(f"Error al procesar y cargar el archivo {file_path}: {e}")
        return pd.DataFrame()
