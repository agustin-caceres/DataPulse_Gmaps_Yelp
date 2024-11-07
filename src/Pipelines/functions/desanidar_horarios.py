from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import logging

###################################################

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