from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import json

################################################################

def crear_tabla_miscelaneos(project_id: str, dataset: str) -> None:
    """
    Crea la tabla 'miscelaneos' en el dataset de BigQuery si no existe.
    
    Args:
    -------
    project_id : str
        ID del proyecto en Google Cloud Platform.
    dataset : str
        Nombre del dataset en BigQuery.
    """
    client = bigquery.Client()
    
    # Definir el ID de la tabla
    miscelaneos_table_id = f"{project_id}.{dataset}.miscelaneos"
    
    # Definir la consulta SQL para crear la tabla 'miscelaneos'
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS `{miscelaneos_table_id}` (
        gmap_id STRING,         
        MISC STRING,
    )
    """
    
    # Ejecutar la consulta para crear la tabla
    try:
        client.query(create_table_query).result()
        print(f"Tabla '{miscelaneos_table_id}' creada con éxito.")
    except Exception as e:
        print(f"Error al crear la tabla '{miscelaneos_table_id}': {e}")

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

    # Carga el archivo JSON en un DataFrame de Pandas
    df = pd.read_json(contenido, lines=True)

    # Filtra registros sin información en 'MISC' o 'gmap_id'
    df = df[df['MISC'].notna() & df['gmap_id'].notna()]

    # Expande la columna 'MISC' usando dict_to_list para cada registro y crea un nuevo DataFrame
    df['misc'] = df['MISC'].apply(dict_to_list)
    df_expanded = df[['gmap_id', 'misc']].explode('misc').dropna()

    # Carga el DataFrame resultante a BigQuery y Verifica si la tabla ya existe y tiene datos
    table = client.get_table(table_id)
    if table.num_rows == 0:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    else:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

    client.load_table_from_dataframe(df_expanded[['gmap_id', 'misc']], table_id, job_config=job_config).result()
    print(f"Datos del archivo {archivo} cargados exitosamente en BigQuery.")

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

def generalizar_atributos(project_id: str, dataset: str) -> None:
    """
    Generaliza los valores de la columna 'atributo' en la tabla de BigQuery para agrupar términos similares.
    
    Args:
    -------
    project_id : str
        ID del proyecto en Google Cloud Platform.
    dataset : str
        Nombre del dataset en BigQuery donde se encuentra la tabla temporal.
    """
    client = bigquery.Client()
    temp_table_id = f"{project_id}.{dataset}.temp_miscelaneos"
    
    # Definir el mapeo para generalizar atributos
    mapeo_lgbtq = {
        'LGBTQ-friendly': 'LGBTQ friendly',
        'Transgender safespace': 'LGBTQ friendly',
        'Transgender safe space': 'LGBTQ friendly'
    }

    mapeo_accesibilidad = {
        'Wheelchair accessible restroom': 'Wheelchair accessible toilet',
        'Wheelchair accessible parking lot': 'Wheelchair accessible car park',
        'Wheelchair accessible lift': 'Wheelchair accessible elevator'
    }
    
    # Para cada clave en los mapeos, se genera una consulta de UPDATE
    for old_value, new_value in mapeo_lgbtq.items():
        update_query = f"""
        UPDATE `{temp_table_id}`
        SET atributo = '{new_value}'
        WHERE atributo = '{old_value}'
        """
        client.query(update_query).result()  # Ejecuta la consulta de actualización

    for old_value, new_value in mapeo_accesibilidad.items():
        update_query = f"""
        UPDATE `{temp_table_id}`
        SET atributo = '{new_value}'
        WHERE atributo = '{old_value}'
        """
        client.query(update_query).result()  # Ejecuta la consulta de actualización

    print("Atributos generalizados con éxito.")


###################################################################################

def marcar_nuevas_accesibilidades(project_id: str, dataset: str) -> None:
    """
    Actualiza la columna 'category' en la tabla de BigQuery para marcar ciertos 
    valores de 'atributo' con la categoría 'Accessibility'.
    
    Args:
    -------
    project_id : str
        ID del proyecto en Google Cloud Platform.
    dataset : str
        Nombre del dataset en BigQuery donde se encuentra la tabla temporal.
    """
    client = bigquery.Client()
    temp_table_id = f"{project_id}.{dataset}.temp_miscelaneos"
    
    # Consulta para actualizar la categoría a 'Accessibility' basado en las condiciones
    update_categoria_query = f"""
    UPDATE `{temp_table_id}`
    SET category = CASE
        WHEN category = 'Offerings' AND atributo = 'Braille menu' THEN 'Accessibility'
        WHEN category = 'Amenities' AND atributo = 'High chairs' THEN 'Accessibility'
        ELSE category
    END
    WHERE (category = 'Offerings' AND atributo = 'Braille menu')
       OR (category = 'Amenities' AND atributo = 'High chairs')
    """
    
    # Ejecutar la consulta de actualización
    client.query(update_categoria_query).result()
    
    print("Categorías de accesibilidad actualizadas con éxito.")

#########################################################################################

def mover_a_tabla_oficial(project_id: str, dataset: str) -> None:
    """
    Mueve los datos desde la tabla temporal 'temp_miscelaneos' a la tabla oficial 'g_misc' en BigQuery,
    añadiendo los registros sin eliminar los anteriores.
    
    Args:
    -------
    project_id : str
        ID del proyecto en Google Cloud Platform.
    dataset : str
        Nombre del dataset en BigQuery donde se encuentran las tablas.
    """
    client = bigquery.Client()
    temp_table_id = f"{project_id}.{dataset}.temp_miscelaneos"
    official_table_id = f"{project_id}.{dataset}.g_misc"
    
    # Consulta SQL para insertar los registros desde la tabla temporal a la tabla oficial
    insert_query = f"""
    INSERT INTO `{official_table_id}` (id_negocio, categoria_atributo, atributo)
    SELECT gmap_id, category, atributo
    FROM `{temp_table_id}`
    """
    
    # Ejecutar la consulta
    insert_query_job = client.query(insert_query)
    insert_query_job.result()  # Espera a que se complete la inserción
    
    print("Datos movidos a la tabla oficial con éxito.")
    
#############################################################################################

def eliminar_tablas_temporales(project_id: str, dataset: str) -> None:
    """
    Elimina las tablas temporales 'temp_miscelaneos' y 'miscelaneos' en BigQuery.
    
    Args:
    -------
    project_id : str
        ID del proyecto en Google Cloud Platform.
    dataset : str
        Nombre del dataset en BigQuery.
    """
    client = bigquery.Client()
    
    # Definir las tablas a eliminar
    temp_table_id = f"{project_id}.{dataset}.temp_miscelaneos"
    miscelaneos_table_id = f"{project_id}.{dataset}.miscelaneos"
    
    # Eliminar la tabla temporal 'temp_miscelaneos'
    drop_temp_query = f"DROP TABLE IF EXISTS `{temp_table_id}`"
    client.query(drop_temp_query).result()  # Ejecuta la consulta y espera el resultado
    print(f"Tabla temporal {temp_table_id} eliminada con éxito.")
    
    # Eliminar la tabla 'miscelaneos' (temporal)
    drop_miscelaneos_query = f"DROP TABLE IF EXISTS `{miscelaneos_table_id}`"
    client.query(drop_miscelaneos_query).result()  # Ejecuta la consulta y espera el resultado
    print(f"Tabla {miscelaneos_table_id} eliminada con éxito.")



