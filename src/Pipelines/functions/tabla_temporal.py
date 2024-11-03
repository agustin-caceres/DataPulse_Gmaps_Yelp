from google.cloud import bigquery
from google.cloud import storage
import json

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
    
    client = bigquery.Client()
    storage_client = storage.Client()

    for archivo in archivos:
        try:
            # Lee el archivo JSON desde Cloud Storage
            blob = storage_client.bucket(bucket_name).blob(archivo)
            contenido = blob.download_as_text()

            # Procesa cada línea como un objeto JSON e inserta en la tabla temporal
            rows_to_insert = []
            for linea in contenido.splitlines():
                try:
                    contenido_json = json.loads(linea)
                    rows_to_insert.append(contenido_json)
                except json.JSONDecodeError as e:
                    print(f"Error de decodificación JSON en la línea: {e}")
                    continue

            # Inserta los datos en la tabla temporal
            table_id = f"{project_id}.{dataset}.{temp_table}"
            if rows_to_insert:
                errors = client.insert_rows_json(table_id, rows_to_insert)
                if errors:
                    print(f"Error al insertar datos del archivo {archivo}: {errors} (Total filas: {len(rows_to_insert)})")
                else:
                    print(f"Datos del archivo {archivo} cargados exitosamente en la tabla temporal.")
            else:
                print(f"No se encontraron datos para cargar del archivo {archivo}.")
        
        except Exception as e:
            print(f"Error al procesar el archivo {archivo}: {str(e)}")


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
