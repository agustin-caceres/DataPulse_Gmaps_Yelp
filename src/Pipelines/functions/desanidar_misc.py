from google.cloud import bigquery
from google.cloud import storage
import json

################################################################ 
def dict_to_list(diccionario: dict) -> list:
    """
    Convierte el diccionario de la columna `MISC` en una lista de strings
    "key: value" excluyendo pares donde el valor es nulo.

    Args:
        diccionario (dict): diccionario recibido.

    Returns:
        list: lista con los pares clave:valor no nulos.
    """
    return [f"{key}: {value}" for key, value in diccionario.items() if value is not None]

################################################################ 

def desanidar_misc(bucket_name: str, archivo: str, project_id: str, dataset: str) -> None:
    """
    Toma un archivo JSON de Google Cloud Storage, extrae los valores de 'MISC' 
    y los guarda desanidados en BigQuery, descartando registros donde 'MISC' es nulo.

    Args:
    -------
    bucket_name : str
        Nombre del bucket en Google Cloud Storage.
    archivo : str
        Nombre del archivo JSON que contiene la columna 'MISC'.
    project_id : str
        ID del proyecto en Google Cloud Platform.
    dataset : str
        Nombre del dataset en BigQuery donde se encuentra la tabla 'miscelaneos'.
    """

    # Inicializa el cliente de BigQuery y el cliente de Cloud Storage
    client = bigquery.Client()
    storage_client = storage.Client()
    
    # Define el ID de la tabla de destino
    table_id = f"{project_id}.{dataset}.miscelaneos"
    
    # Lee el archivo JSON desde Cloud Storage
    blob = storage_client.bucket(bucket_name).blob(archivo)
    contenido = blob.download_as_text()

    # Procesa cada línea del archivo como un objeto JSON
    for linea in contenido.splitlines():
        try:
            contenido_json = json.loads(linea)
        except json.JSONDecodeError as e:
            print(f"Error de decodificación JSON en la línea: {e}")
            continue

        # Filtra registros sin información en 'MISC'
        gmap_id = contenido_json.get('gmap_id')
        misc = contenido_json.get('MISC')

        if gmap_id is None or misc is None:
            print(f"Registro sin 'gmap_id' o sin 'MISC' en archivo {archivo}. Se omite.")
            continue

        # Convierte el diccionario `MISC` en una lista usando `dict_to_list`
        lista_misc = dict_to_list(misc)
        rows_to_insert = [{"gmap_id": gmap_id, "misc": item} for item in lista_misc]

        # Inserta los datos desanidados en BigQuery
        if rows_to_insert:
            errors = client.insert_rows_json(table_id, rows_to_insert)
            if errors:
                print(f"Error al insertar datos en BigQuery: {errors}")
            else:
                print(f"Datos del archivo {archivo} cargados exitosamente en BigQuery.")
        else:
            print(f"Sin datos para insertar del archivo {archivo}.")

###########################################################################################

from google.cloud import bigquery

def actualizar_misc_con_atributos(project_id: str, dataset: str) -> None:
    """
    Actualiza la tabla 'miscelaneos' en BigQuery, creando una nueva tabla temporal que agrega
    las columnas 'category' y 'atributo' a partir de la columna 'MISC'.

    Args:
    -------
    project_id : str
        ID del proyecto en Google Cloud Platform.
    dataset : str
        Nombre del dataset en BigQuery donde se encuentra la tabla 'miscelaneos'.
    """
    client = bigquery.Client()
    table_id = f"{project_id}.{dataset}.miscelaneos"
    temp_table_id = f"{project_id}.{dataset}.temp_miscelaneos"

    # Consulta SQL para crear la tabla temporal con los datos procesados
    query = f"""
    CREATE OR REPLACE TABLE `{temp_table_id}` AS
    WITH updated_misc AS (
        SELECT 
            gmap_id,
            -- Extraer la categoría antes del primer ':'
            REGEXP_EXTRACT(MISC, r"^(.*?):") AS category,  
            -- Extraer la lista después del ':', eliminando los corchetes y comillas
            REGEXP_EXTRACT(MISC, r":\s*(\[[^\]]*\])") AS atributo  -- Extrae la lista
        FROM `{table_id}`
        WHERE MISC IS NOT NULL
    ),
    exploded AS (
        SELECT 
            gmap_id,
            category,
            -- Desanidar la lista de atributo separando por comas
            TRIM(REGEXP_EXTRACT(atributo, r"'(.*?)'")) AS atributo_raw
        FROM updated_misc
        WHERE atributo IS NOT NULL
    ),
    -- Ahora, separamos la lista de elementos por coma y lo expandimos en filas
    final_exploded AS (
        SELECT 
            gmap_id,
            category,
            -- Separamos por coma para obtener los elementos individuales
            TRIM(element) AS atributo
        FROM exploded,
        UNNEST(SPLIT(atributo_raw, ',')) AS element
    )
    SELECT gmap_id, category, atributo
    FROM final_exploded
    """

    # Ejecuta la consulta para crear la tabla temporal
    extract_query_job = client.query(query)
    extract_query_job.result()  # Espera a que termine la consulta

    print(f"Tabla temporal '{temp_table_id}' creada con éxito.")

##################################################################################

from google.cloud import bigquery

def eliminar_categorias_especificas(project_id: str, dataset: str) -> None:
    """
    Elimina filas con categorías específicas ('Health & safety')
    en la tabla temporal en BigQuery.

    Args:
    -------
    project_id : str
        ID del proyecto en Google Cloud Platform.
    dataset : str
        Nombre del dataset en BigQuery donde se encuentra la tabla temporal.
    """
    client = bigquery.Client()
    temp_table_id = f"{project_id}.{dataset}.temp_miscelaneos"
    
    # Consulta SQL para eliminar las filas con las categorías específicas
    delete_query = f"""
    DELETE FROM `{temp_table_id}`
    WHERE category IN ('Health & safety')
    """
    
    # Ejecuta la consulta de eliminación
    delete_query_job = client.query(delete_query)
    delete_query_job.result()  # Espera a que se complete la eliminación
    
    print("Filas eliminadas con éxito.")
    
##################################################################################





