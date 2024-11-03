from google.cloud import bigquery
from google.cloud import storage
import json
import pandas as pd
from io import BytesIO

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

def cargar_archivos_en_tabla_temporal(bucket_name: str, archivos: list, project_id: str, dataset: str, temp_table: str) -> None:
    """
    Carga múltiples archivos JSON desde Google Cloud Storage a la tabla temporal en BigQuery.
    """

    # Inicializa el cliente de BigQuery y el cliente de Cloud Storage
    client = bigquery.Client()
    storage_client = storage.Client()

    if not archivos or not isinstance(archivos, list):
        raise ValueError("La lista de archivos está vacía o no es válida.")

    for archivo in archivos:
        try:
            # Lee el archivo JSON desde Cloud Storage
            blob = storage_client.bucket(bucket_name).blob(archivo)
            contenido = blob.download_as_text()

            # Carga todo el archivo como un JSON
            contenido_json = json.loads(contenido)
            
            # Asegura que es una lista de objetos JSON
            if not isinstance(contenido_json, list):
                raise ValueError(f"El archivo {archivo} no contiene un array de objetos JSON.")

            # Inserta los datos en la tabla temporal
            table_id = f"{project_id}.{dataset}.{temp_table}"
            errors = client.insert_rows_json(table_id, contenido_json)
            
            if errors:
                raise RuntimeError(f"Error al insertar datos del archivo {archivo}: {errors}")
            
            print(f"Datos del archivo {archivo} cargados exitosamente en la tabla temporal.")
        
        except json.JSONDecodeError as e:
            print(f"Error de decodificación JSON en el archivo {archivo}: {e}")
        except Exception as e:
            print(f"Error al procesar el archivo {archivo}: {e}")
    
    # Cierra los clientes de BigQuery
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


def cargar_archivos_en_tabla_temporal_v_premium(bucket_name: str, archivos: list, project_id: str, dataset: str, temp_table: str) -> None:
    """
    Carga múltiples archivos (JSON, Parquet, PKL) desde Google Cloud Storage a la tabla temporal en BigQuery.
    """
    # Imprimir el tipo y contenido completo de `archivos`
    print(f"Tipo de 'archivos' recibido: {type(archivos)}")
    print(f"Contenido de 'archivos' recibido: {archivos}")

    # Verificación inicial de la lista de archivos
    if not isinstance(archivos, list):
        print("Error: 'archivos' no es una lista válida.")
        raise ValueError("La lista de archivos no es válida.")
    
    if len(archivos) == 0 or all(archivo.strip() == "" for archivo in archivos):
        print("Error: La lista de archivos está vacía o contiene entradas inválidas.")
        raise ValueError("La lista de archivos está vacía o contiene entradas inválidas.")
    
    client = bigquery.Client()
    storage_client = storage.Client()
    table_id = f"{project_id}.{dataset}.{temp_table}"

    for archivo in archivos:
        try:
            # Verificar que el archivo no sea solo el prefijo del directorio
            if archivo.endswith('/'):
                print(f"Advertencia: {archivo} parece ser un directorio y no un archivo. Saltando...")
                continue

            # Leer el archivo desde GCS según su formato
            blob = storage_client.bucket(bucket_name).blob(archivo)
            print(f"Leyendo archivo {archivo} desde GCS...")

            # Procesar según el formato
            if archivo.endswith(".json"):
                contenido = blob.download_as_text()
                contenido_json = json.loads(contenido)

                # Verificar que el contenido JSON es una lista de objetos
                if not isinstance(contenido_json, list):
                    raise ValueError(f"El archivo {archivo} no contiene un array de objetos JSON.")
                
                datos = contenido_json

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
