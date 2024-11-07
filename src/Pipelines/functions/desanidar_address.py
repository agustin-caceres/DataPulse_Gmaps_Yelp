from google.cloud import bigquery
from google.cloud import storage
from io import StringIO
import pandas as pd
import logging

####################################

# Configuración básica del logging
logging.basicConfig(level=logging.INFO)

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
