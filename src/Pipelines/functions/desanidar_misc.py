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

################################################################ 

def actualizar_misc_con_atributos(project_id: str, dataset: str) -> None:
    """
    Actualiza la tabla 'miscelaneos' en BigQuery, agregando las columnas 'category' y 'atributo'
    a partir de la columna 'misc', además de renombrar 'gmap_id' a 'id_negocio'.

    Args:
    -------
    project_id : str
        ID del proyecto en Google Cloud Platform.
    dataset : str
        Nombre del dataset en BigQuery donde se encuentra la tabla 'miscelaneos'.
    """
    client = bigquery.Client()
    table_id = f"{project_id}.{dataset}.miscelaneos"

    # Consulta SQL para extraer y preparar los datos
    query = f"""
    WITH updated_misc AS (
        SELECT 
            id_negocio,
            SPLIT(misc, ':')[SAFE_OFFSET(0)] AS category,  -- Extrae la categoría
            TRIM(SPLIT(misc, ':')[SAFE_OFFSET(1)]) AS atributo  -- Extrae el contenido después de ':'
        FROM `{table_id}`
        WHERE misc IS NOT NULL
    ),
    exploded AS (
        SELECT 
            id_negocio,
            category,
            REGEXP_EXTRACT(atributo, r"'(.*?)'") AS atributo  -- Extrae los textos dentro de comillas
        FROM updated_misc
        WHERE atributo IS NOT NULL
    )
    SELECT id_negocio, category, atributo
    FROM exploded
    """

    # Ejecuta la consulta para extraer los datos preparados
    extract_query_job = client.query(query)
    results = extract_query_job.result()  # Espera a que termine la consulta

    # Inserta los datos en la tabla original, desanidando las filas
    insert_query = f"""
    INSERT INTO `{table_id}` (id_negocio, category, atributo)
    VALUES ({','.join([f"('{row.id_negocio}', '{row.category}', '{row.atributo}')" for row in results])})
    """

    # Ejecuta la consulta de inserción
    insert_query_job = client.query(insert_query)
    insert_query_job.result()  # Espera a que termine la consulta
    print("Tabla 'miscelaneos' actualizada con éxito.")
