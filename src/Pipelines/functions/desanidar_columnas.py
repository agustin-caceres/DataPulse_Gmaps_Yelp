from google.cloud import bigquery, storage
from io import StringIO
import pandas as pd
import logging

# Configuración básica del logging
logging.basicConfig(level=logging.INFO)

################################################################################################
def desanidar_columna(bucket_name: str, archivo: str, project_id: str, dataset: str, columna: str, tabla_destino: str) -> None:
    """
    Toma un archivo JSON de Google Cloud Storage, extrae los valores de una columna específica 
    y los guarda desanidados en BigQuery.

    Args:
    -------
    bucket_name : str
        Nombre del bucket en Google Cloud Storage.
    archivo : str
        Nombre del archivo JSON que contiene la columna a desanidar.
    project_id : str
        ID del proyecto en Google Cloud Platform.
    dataset : str
        Nombre del dataset en BigQuery donde se guardará la tabla destino.
    columna : str
        Nombre de la columna a desanidar.
    tabla_destino : str
        Nombre de la tabla en BigQuery donde se guardarán los datos desanidados.
    """
    
    # Inicializa los clientes de BigQuery y Cloud Storage
    client = bigquery.Client()
    storage_client = storage.Client()

    # Define el ID de la tabla de destino
    table_id = f"{project_id}.{dataset}.{tabla_destino}"

    # Lee el archivo JSON desde Cloud Storage
    blob = storage_client.bucket(bucket_name).blob(archivo)
    contenido = blob.download_as_text()

    # Carga el archivo JSON en un DataFrame de Pandas
    df = pd.read_json(contenido, lines=True)

    # Filtra registros sin información en la columna seleccionada o en 'gmap_id'
    df = df[df[columna].notna() & df['gmap_id'].notna()]

    # Expande la columna seleccionada usando explode
    df_expanded = df[['gmap_id', columna]].explode(columna).dropna()

    # Carga el DataFrame resultante a BigQuery y verifica si la tabla ya existe y tiene datos
    table = client.get_table(table_id)
    
    if table.num_rows == 0:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    else:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

    client.load_table_from_dataframe(df_expanded[['gmap_id', columna]], table_id, job_config=job_config).result()
    logging.info(f"Datos del archivo {archivo} cargados exitosamente en la tabla {tabla_destino} de BigQuery.")

#########################################################################################################

def seleccionar_columnas(bucket_name: str, archivo: str, project_id: str, dataset: str) -> None:
    """
    Toma un archivo JSON de Google Cloud Storage, filtra los registros que contienen datos que querramos 
    y los sube a BigQuery en caso de que cumplan con los criterios establecidos.

    Args:
    -------
    bucket_name : str
        Nombre del bucket en Google Cloud Storage.
    archivo : str
        Nombre del archivo JSON a procesar.
    project_id : str
        ID del proyecto en Google Cloud Platform.
    dataset : str
        Nombre del dataset en BigQuery donde se encuentra la tabla 'miscelaneos'.
    """

    # Inicializa los clientes de BigQuery y Cloud Storage
    client = bigquery.Client()
    storage_client = storage.Client()

    # Define el ID de la tabla de destino
    table_id = f"{project_id}.{dataset}.g_sitios"

    # Lee el archivo JSON desde Cloud Storage
    blob = storage_client.bucket(bucket_name).blob(archivo)
    contenido = blob.download_as_text()

    # Carga el archivo JSON en un DataFrame de Pandas
    df = pd.read_json(contenido, lines=True)

    # Filtra registros sin información en 'MISC' o 'gmap_id'
    df = df[df['name'].notna() & df['gmap_id'].notna()]

    # Selecciona solo las columnas necesarias sin explotar/desanidar
    df_selected = df[['gmap_id', 'name', 'description', 'url', 'avg_rating', 'num_of_reviews']]

    # Configura la carga en BigQuery, utilizando "WRITE_APPEND" si la tabla ya contiene datos
    table = client.get_table(table_id)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND" if table.num_rows > 0 else "WRITE_TRUNCATE")

    # Carga los datos en BigQuery
    client.load_table_from_dataframe(df_selected, table_id, job_config=job_config).result()
    print(f"Datos del archivo {archivo} cargados exitosamente en BigQuery.")
    
####################################################################################################################
    
def desanidar_horarios(bucket_name: str, archivo: str, project_id: str, dataset: str) -> None:
    """
    Toma un archivo JSON de Google Cloud Storage, extrae los valores de 'hours' 
    y los guarda desanidados en BigQuery con los horarios de cada día de la semana.

    Args:
    -------
    bucket_name : str
        Nombre del bucket en Google Cloud Storage.
    archivo : str
        Nombre del archivo JSON que contiene la columna 'hours'.
    project_id : str
        ID del proyecto en Google Cloud Platform.
    dataset : str
        Nombre del dataset en BigQuery donde se guardará la tabla 'horarios'.
    """
    
    # Inicializa el cliente de BigQuery y el cliente de Cloud Storage
    client = bigquery.Client()
    storage_client = storage.Client()

    # Define el ID de la tabla de destino
    table_id = f"{project_id}.{dataset}.g_horarios"

    # Lee el archivo JSON desde Cloud Storage
    blob = storage_client.bucket(bucket_name).blob(archivo)
    contenido = blob.download_as_text()

    # Carga el archivo JSON en un DataFrame de Pandas
    df = pd.read_json(contenido, lines=True)

    # Filtra registros sin información en 'hours' o 'gmap_id'
    df = df[df['hours'].notna() & df['gmap_id'].notna()]

    # Función interna para extraer horarios de cada día
    def retornar_horario(campo: list, dia: str) -> str:
        try:
            for i in range(len(campo)):
                if campo[i][0].lower() == dia.lower():
                    return campo[i][1]
            return 'No Disponible'
        except IndexError:
            return 'No Disponible'

    # Desanida horarios para cada día de la semana
    for dia in ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']:
        df[dia] = df['hours'].apply(lambda x: retornar_horario(x, dia=dia.capitalize()))

    # Selecciona las columnas necesarias
    df_expanded = df[['gmap_id', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']]

    # Configura el trabajo de carga en BigQuery y selecciona la disposición de escritura
    table = client.get_table(table_id)
    if table.num_rows == 0:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    else:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

    # Carga el DataFrame resultante a BigQuery
    client.load_table_from_dataframe(df_expanded, table_id, job_config=job_config).result()
    logging.info(f"Datos del archivo {archivo} cargados exitosamente en la tabla 'horarios' de BigQuery.")  
    
#####################################################################################################################

def desanidar_address(bucket_name: str, archivo: str, project_id: str, dataset: str) -> None:
    """
    Toma un archivo JSON de Google Cloud Storage, extrae las direcciones y las separa en columnas adicionales.
    Luego, guarda los registros desanidados en BigQuery, descartando registros donde 'address' o 'gmap_id' son nulos.
    """
    # Inicializa el cliente de BigQuery y el cliente de Cloud Storage
    client = bigquery.Client(project=project_id)
    storage_client = storage.Client()

    # Define el ID de la tabla de destino
    table_id = f"{project_id}.{dataset}.g_address"

    # Lee el archivo JSON desde Cloud Storage
    blob = storage_client.bucket(bucket_name).blob(archivo)
    contenido = blob.download_as_text()
    # Usa StringIO para pasar el contenido como un archivo
    contenido_io = StringIO(contenido)

    # Carga el archivo JSON en un DataFrame de Pandas
    try:
        df = pd.read_json(contenido_io, lines=True)
        logging.info(f"Archivo {archivo} cargado exitosamente en un DataFrame.")
    except ValueError as e:
        logging.error(f"Error al cargar el archivo {archivo} en un DataFrame: {e}")
        raise  # Re-lanzar la excepción para que el flujo falle correctamente

    # Filtra registros sin información en 'address' o 'gmap_id'
    df = df[df['address'].notna() & df['gmap_id'].notna()]

    # Separar la columna 'address' en nuevas columnas
    address_split = df['address'].str.split(',', n=3, expand=True)
    address_split.columns = ['nombre', 'direccion', 'ciudad', 'cod_postal']

    # Agregar las columnas separadas al DataFrame
    df = df[['gmap_id', 'latitude', 'longitude', 'address']].join(address_split, how="inner")
    
    # Agregar una columna nueva llamada estados.
    df['estado'] = None

    # Listas de los 51 códigos postales y nombres de estados
    codigos_postales = [
        'CA', 'NY', 'IA', 'GA', 'FL', 'TX', 'LA', 'OR', 'WV', 'VA', 
        'AR', 'PA', 'NM', 'NC', 'TN', 'WI', 'NJ', 'IN', 'IL', 'DC', 
        'MD', 'ME', 'NE', 'WA', 'MI', 'OH', 'OK', 'MO', 'KS', 'UT', 
        'HI', 'NV', 'AZ', 'AL', 'CO', 'MA', 'ID', 'SC', 'RI', 'KY', 
        'AK', 'MT', 'MN', 'CT', 'MS', 'SD', 'WY', 'NH', 'DE', 'VT', 
        'ND'
    ]
    nombres_estados = [
        'California', 'New York', 'Iowa', 'Georgia', 'Florida', 'Texas', 
        'Louisiana', 'Oregon', 'West Virginia', 'Virginia', 'Arkansas', 
        'Pennsylvania', 'New Mexico', 'North Carolina', 'Tennessee', 'Wisconsin', 
        'New Jersey', 'Indiana', 'Illinois', 'District of Columbia', 'Maryland', 
        'Maine', 'Nebraska', 'Washington', 'Michigan', 'Ohio', 'Oklahoma', 
        'Missouri', 'Kansas', 'Utah', 'Hawaii', 'Nevada', 'Arizona', 
        'Alabama', 'Colorado', 'Massachusetts', 'Idaho', 'South Carolina', 
        'Rhode Island', 'Kentucky', 'Alaska', 'Montana', 'Minnesota', 
        'Connecticut', 'Mississippi', 'South Dakota', 'Wyoming', 
        'New Hampshire', 'Delaware', 'Vermont', 'North Dakota'
    ]
    
    # Mapeo directo de códigos postales a estados
    postal_to_state = dict(zip(codigos_postales, nombres_estados))
    df['estado'] = df['cod_postal'].map(postal_to_state).fillna(df['estado'])
    
    # Realizar limpieza en base a la ciudad y código postal
    for codigo, estado in zip(codigos_postales, nombres_estados):
        df.loc[(df['cod_postal'].isnull()) & (df['ciudad'].str.contains(codigo, na=False)), 'estado'] = estado

    # Eliminar filas donde el estado no puede ser determinado
    df = df.dropna(subset=['estado'])

    # Agregamos un identificador único a cada estado
    df['id_estado'] = df['estado'].factorize()[0] + 1
    print(df.columns)
    # Selecciona las columnas necesarias
    df_expanded = df[['gmap_id', 'address', 'latitude', 'longitude', 'direccion', 'ciudad', 'cod_postal', 'estado', 'id_estado']]

    # Cargar los datos a BigQuery
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND" if table_id else "WRITE_TRUNCATE")
    try:
        load_job = client.load_table_from_dataframe(df_expanded, table_id, job_config=job_config)
        load_job.result()  # Espera a que la carga termine
        logging.info(f"Datos cargados exitosamente a {table_id}.")
    except Exception as e:
        logging.error(f"Error al cargar los datos a BigQuery: {e}")
        raise  # Re-lanzar la excepción para que el flujo falle correctamente  
    

###########################################################################
# MISCELANEOS
###########################################################################

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
    Actualiza la tabla 'misceláneos' en BigQuery, creando una nueva tabla temporal que agrega
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
    INSERT INTO `{official_table_id}` (gmap_id, categoria_atributo, atributo)
    SELECT gmap_id, category, atributo
    FROM `{temp_table_id}`
    """
    
    # Ejecutar la consulta
    insert_query_job = client.query(insert_query)
    insert_query_job.result()  # Espera a que se complete la inserción
    
    print("Datos movidos a la tabla oficial con éxito.")
    
#############################################################################################
