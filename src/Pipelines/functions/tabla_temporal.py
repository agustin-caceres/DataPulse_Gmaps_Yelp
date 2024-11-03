from google.cloud import bigquery
from google.cloud import storage
import json
import pandas as pd
from io import BytesIO
from io import StringIO

###########################################################################

def crear_tabla_temporal(project_id: str, dataset: str, temp_table: str, schema: list) -> str:
    """
    Crea una tabla temporal en BigQuery con un esquema dado.

    Parámetros:
    -----------
    project_id : str
        ID del proyecto en Google Cloud.
    dataset : str
        Nombre del dataset en BigQuery.
    temp_table : str
        Nombre de la tabla temporal a crear.
    schema : list
        Esquema de la tabla temporal, como una lista de bigquery.SchemaField.

    Retorna:
    --------
    str
        Mensaje indicando que la tabla temporal fue creada.
    """
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset}.{temp_table}"
    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table, exists_ok=True)
    return f"Tabla temporal {table_id} creada."

###########################################################################

def cargar_archivo_en_tabla_temporal(bucket_name: str, archivo: str, project_id: str, dataset: str, temp_table: str) -> None:
    """
    Carga un archivo JSON en formato DataFrame desde Google Cloud Storage a la tabla temporal en BigQuery.
    
    Args:
        bucket_name (str): Nombre del bucket de Google Cloud Storage.
        archivo (str): Nombre del archivo.
        project_id (str): ID del proyecto de Google Cloud.
        dataset (str): Nombre del dataset de BigQuery.
        temp_table (str): Nombre de la tabla temporal en BigQuery.
    """

    # Inicializa el cliente de BigQuery y el cliente de Cloud Storage
    client = bigquery.Client()
    storage_client = storage.Client()

    # Lee el archivo JSON desde Cloud Storage
    blob = storage_client.bucket(bucket_name).blob(archivo)
    contenido = blob.download_as_text()

    # Carga el contenido del archivo en un DataFrame de pandas
    df = pd.read_json(StringIO(contenido), lines=True)

    # Limpiar y convertir la columna 'hours'
    df['hours'] = df['hours'].astype(str)  
    df['hours'] = df['hours'].fillna('')
    
    # Asegura que el DataFrame no está vacío
    if df.empty:
        raise ValueError(f"El archivo {archivo} no contiene datos.")

    # Inserta los datos en la tabla temporal
    table_id = f"{project_id}.{dataset}.{temp_table}"
    job = client.load_table_from_dataframe(df, table_id)

    # Espera a que se complete el trabajo de carga
    job.result()  # Esto bloqueará hasta que el trabajo se complete

    if job.error_result:
        raise RuntimeError(f"Error al insertar datos del archivo {archivo}: {job.error_result}")

    # Cierra el cliente de BigQuery
    client.close()

###########################################################################

def mover_datos_y_borrar_temp(project_id: str, dataset: str, temp_table: str, final_table: str) -> str:
    """
    Mueve los datos de una tabla temporal a una tabla final y elimina la temporal.

    Parámetros:
    -----------
    project_id : str
        ID del proyecto en Google Cloud.
    dataset : str
        Nombre del dataset en BigQuery.
    temp_table : str
        Nombre de la tabla temporal que se va a mover y eliminar.
    final_table : str
        Nombre de la tabla final en BigQuery donde se moverán los datos.

    Retorna:
    --------
    str
        Mensaje indicando que los datos fueron movidos y la tabla temporal fue eliminada.
    """
    client = bigquery.Client(project=project_id)
    
    # Mueve datos de la tabla temporal a la tabla final
    query_move = f"""
    INSERT INTO `{project_id}.{dataset}.{final_table}`
    SELECT * FROM `{project_id}.{dataset}.{temp_table}`
    """
    client.query(query_move).result()
    
    # Elimina la tabla temporal después de mover los datos
    table_id = f"{project_id}.{dataset}.{temp_table}"
    client.delete_table(table_id, not_found_ok=True)
    return f"Datos movidos a {final_table} y tabla temporal {temp_table} eliminada."


###########################################################################


def cargar_archivos_en_tabla_temporal_v_premium(bucket_name: str, archivos, project_id: str, dataset: str, temp_table: str) -> None:
    """
    Carga múltiples archivos (JSON, Parquet, PKL) desde Google Cloud Storage a la tabla temporal en BigQuery.
    """
    # Imprimir el tipo y contenido de `archivos`
    print(f"Tipo de 'archivos' recibido: {type(archivos)}")
    print(f"Contenido de 'archivos' recibido: {archivos}")

    # Verificación inicial de `archivos` y conversión si es necesario
    if isinstance(archivos, str):
        try:
            archivos = json.loads(archivos)  # Convertir a lista si es un JSON en forma de string
        except json.JSONDecodeError as e:
            raise ValueError("Error al decodificar 'archivos'. Asegúrate de que sea una lista válida.") from e

    if not isinstance(archivos, list) or not archivos:
        raise ValueError("La lista de archivos no es válida o está vacía.")

    client = bigquery.Client()
    storage_client = storage.Client()
    table_id = f"{project_id}.{dataset}.{temp_table}"

    for archivo in archivos:
        try:
            if archivo.endswith('/'):
                print(f"Advertencia: {archivo} parece ser un directorio y no un archivo. Saltando...")
                continue

            blob = storage_client.bucket(bucket_name).blob(archivo)
            print(f"Leyendo archivo {archivo} desde GCS...")

            if archivo.endswith(".json"):
                contenido = blob.download_as_text()
                # Manejo de JSON en múltiples líneas (objetos JSON separados por línea)
                datos = []
                for line in contenido.strip().splitlines():
                    try:
                        datos.append(json.loads(line))
                    except json.JSONDecodeError:
                        print(f"Error en línea JSON en archivo {archivo}: {line}")
                        continue  # Saltar líneas mal formadas

            elif archivo.endswith(".parquet"):
                contenido = blob.download_as_bytes()
                df = pd.read_parquet(BytesIO(contenido))
                datos = df.to_dict(orient="records")

            elif archivo.endswith(".pkl"):
                contenido = blob.download_as_bytes()
                df = pd.read_pickle(BytesIO(contenido))
                datos = df.to_dict(orient="records")

            else:
                print(f"Advertencia: Formato de archivo no soportado ({archivo}). Saltando...")
                continue

            # Insertar datos en la tabla temporal
            print(f"Cargando datos del archivo {archivo} en la tabla temporal {temp_table}...")
            errors = client.insert_rows_json(table_id, datos)
            if errors:
                raise RuntimeError(f"Error al insertar datos del archivo {archivo}: {errors}")
            
            print(f"Datos del archivo {archivo} cargados exitosamente en la tabla temporal.")

        except Exception as e:
            print(f"Error al procesar el archivo {archivo}: {e}")
            raise  # Relanzar el error para que Airflow lo maneje

