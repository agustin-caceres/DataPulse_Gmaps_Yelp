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

def transformar_horas(data):
    """
    Transforma la estructura del campo 'hours' en el formato requerido por BigQuery.

    Args:
        data (dict): Un diccionario que representa un registro.

    Returns:
        dict: Un diccionario modificado con el campo 'hours' en el formato correcto.
    """
    # Verifica si 'hours' existe y es del tipo adecuado
    if 'hours' in data and isinstance(data['hours'], list):
        # Transforma la lista de listas en la lista de registros
        data['hours'] = [
            {"day": hour[0], "time": hour[1]} for hour in data['hours']
        ]
    return data

def cargar_json_a_bigquery(bucket_name: str, archivo: str, project_id: str, dataset: str, temp_table: str, schema: list) -> None:
    """
    Carga un archivo JSON desde Google Cloud Storage a una tabla en BigQuery.
    
    Args:
        bucket_name (str): Nombre del bucket de Google Cloud Storage.
        archivo (str): Nombre del archivo JSON.
        project_id (str): ID del proyecto de Google Cloud.
        dataset (str): Nombre del dataset de BigQuery.
        temp_table (str): Nombre de la tabla en BigQuery.
        schema (list): Esquema de la tabla de BigQuery.
    """

    # Inicializa el cliente de BigQuery y de Google Cloud Storage
    bigquery_client = bigquery.Client()
    storage_client = storage.Client()

    # Lee el archivo JSON desde Google Cloud Storage
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(archivo)
    json_data = blob.download_as_text()

    # Carga y transforma el JSON
    data_list = json.loads(json_data)
    transformed_data = [transformar_horas(data) for data in data_list]

    # Crea la tabla si no existe
    table_id = f"{project_id}.{dataset}.{temp_table}"
    table = bigquery.Table(table_id, schema=schema)
    table = bigquery_client.create_table(table, exists_ok=True)  # Si existe, no hace nada

    # Carga el archivo JSON transformado a la tabla
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND  # Cambia según lo que necesites
    )

    # Convierte la lista de diccionarios a un formato que BigQuery puede cargar
    temp_blob = f"temp_{archivo}"
    temp_blob_obj = bucket.blob(temp_blob)
    temp_blob_obj.upload_from_string("\n".join(json.dumps(record) for record in transformed_data))

    # Construye la URI del archivo temporal en Google Cloud Storage
    uri = f"gs://{bucket_name}/{temp_blob}"

    # Carga el archivo JSON transformado
    load_job = bigquery_client.load_table_from_uri(uri, table_id, job_config=job_config)

    # Espera a que se complete el trabajo de carga
    load_job.result()  # Esto bloqueará hasta que el trabajo se complete

    if load_job.error_result:
        raise RuntimeError(f"Error al cargar el archivo JSON {archivo}: {load_job.error_result}")

    print(f"Archivo {archivo} cargado exitosamente en {table_id}.")

    # Limpia el blob temporal después de la carga
    temp_blob_obj.delete()
    
    
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

