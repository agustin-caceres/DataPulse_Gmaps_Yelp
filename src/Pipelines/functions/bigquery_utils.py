import pandas as pd
import io
import time
from google.cloud import bigquery, storage

def crear_tabla_temporal(project_id: str, dataset: str, temp_table: str, schema: list) -> None:
    """
    Crea una tabla temporal en BigQuery con el esquema especificado.
    """
    try:
        client = bigquery.Client(project=project_id)
        table_id = f"{project_id}.{dataset}.{temp_table}"
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table, exists_ok=True)
        print(f"Tabla temporal '{table_id}' creada o ya existente.")

    except Exception as e:
        print(f"Error al crear la tabla temporal {temp_table}: {e}")

def cargar_archivo_gcs_a_bigquery(bucket_name: str, file_path: str, project_id: str, dataset: str, table_name: str, chunk_size: int = 50000) -> None:
    """
    Extrae un archivo desde Google Cloud Storage, lo procesa y lo carga en BigQuery en chunks.
    """
    try:
        # Conexión a GCS y descarga del archivo
        client = storage.Client()
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(file_path)
        data = blob.download_as_text()

        # Cargar archivo en DataFrame
        if file_path.endswith('.json'):
            df = pd.read_json(io.StringIO(data), lines=True)
            if 'checkin.json' in file_path:
                df = df.assign(date=df['date'].str.split(', ')).explode('date').reset_index(drop=True)
        elif file_path.endswith('.parquet'):
            df = pd.read_parquet(io.BytesIO(blob.download_as_bytes()))
        elif file_path.endswith('.pkl'):
            df = pd.read_pickle(io.BytesIO(blob.download_as_bytes()))
        else:
            print("Formato de archivo no soportado.")
            return

        # Cargar DataFrame en BigQuery en chunks
        client_bq = bigquery.Client(project=project_id)
        table_id = f"{project_id}.{dataset}.{table_name}"

        if df.empty:
            print("El DataFrame está vacío. No se cargarán datos en BigQuery.")
            return

        for i in range(0, len(df), chunk_size):
            chunk = df.iloc[i:i + chunk_size]
            job = client_bq.load_table_from_dataframe(chunk, table_id)
            job.result()
            print(f"Cargado chunk {i // chunk_size + 1} de {len(df) // chunk_size + 1}")
            time.sleep(0.5)  # Agregar un pequeño delay entre cargas

    except Exception as e:
        print(f"Error al procesar y cargar el archivo {file_path}: {e}")
