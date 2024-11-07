from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import logging

####################################

def desanidar_address(bucket_name: str, archivo: str, project_id: str, dataset: str) -> None:
    """
    Toma un archivo JSON de Google Cloud Storage, extrae las direcciones y las separa en columnas adicionales.
    Luego, guarda los registros desanidados en BigQuery, descartando registros donde 'address' o 'gmap_id' son nulos.

    Args:
    -------
    bucket_name : str
        Nombre del bucket en Google Cloud Storage.
    archivo : str
        Nombre del archivo JSON que contiene la columna 'address'.
    project_id : str
        ID del proyecto en Google Cloud Platform.
    dataset : str
        Nombre del dataset en BigQuery donde se guardarán los registros desanidados.
    """
    
    # Inicializa el cliente de BigQuery y el cliente de Cloud Storage
    client = bigquery.Client(project=project_id)
    storage_client = storage.Client()

    # Define el ID de la tabla de destino
    table_id = f"{project_id}.{dataset}.address"

    # Lee el archivo JSON desde Cloud Storage
    blob = storage_client.bucket(bucket_name).blob(archivo)
    contenido = blob.download_as_text()

    # Carga el archivo JSON en un DataFrame de Pandas
    df = pd.read_json(contenido, lines=True)
    # Filtra registros sin información en 'address' o 'gmap_id'
    df = df[df['address'].notna() & df['gmap_id'].notna()]
    # Separar la columna 'address' en nuevas columnas
    address_split = df['address'].str.split(',', n=3, expand=True)
    # Cambiar el nombre de las columnas
    address_split.columns = ['nombre', 'direccion', 'ciudad', 'cod.postal']
    # Agregar la identificacion unica a las nuevas columnas separadas.
    df = df[['gmap_id']].join(address_split)
    # Agrega una nueva columna estado.
    df['estado'] = None
    
    # Listas de los 51 códigos postales y nombres de estados.
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
    
    # cod.postal contiene las letras del estado, podemos filtrar los estados rapidamente, a travez de un bucle entre codigo y nombres.
    for codigo, estado in zip(codigos_postales, nombres_estados):
        df.loc[df['cod.postal'].str.contains(codigo, na=False), 'estado'] = estado

    # El cod.postal puede ser nulo por falta de informacion ya que se corre la informacion hacia la izquierda.
    # Para solucionar esto filtramos los cod.postales nulos que se corrieron hacia la izquierda
    for codigo, estado in zip(codigos_postales, nombres_estados):
        df.loc[(df['cod.postal'].isnull()) & (df['ciudad'].str.contains(codigo, na=False)), 'estado'] = estado
        
    # Ahora que estan corregidos las mayorias de los cod.postal algunos datos se cargaron al reves, conteniendo el cod en nombre y el nombre en cod.postal.
    for estado in nombres_estados:
        df.loc[(df['estado'].isnull()) & (df['nombre'].str.contains(estado, na=False)), 'estado'] = estado
        
    # Eliminar filas donde al menos 5 columnas son nulas
    columnas = ['gmap_id', 'nombre', 'direccion', 'ciudad', 'cod.postal', 'estado']
    df = df.dropna(thresh=len(columnas) - 1)
    
    # Ultimas condicionales: el cod.postal no es nulo, pero su codigo aun se encuentra en la ciudad.
    for codigo, estado in zip(codigos_postales, nombres_estados):
        df.loc[(df['cod.postal'].notnull()) & (df['ciudad'].str.contains(codigo, na=False)), 'estado'] = estado

    # El cod.postal no es nulo, pero esta escrito el estado en la ciudad.  
    for codigo, estado in zip(codigos_postales, nombres_estados):
        df.loc[(df['cod.postal'].notnull()) & (df['ciudad'].str.contains(estado, na=False)), 'estado'] = estado
        
    # El cod.postal contiene escrito el estado y no un codigo.  
    for codigo, estado in zip(codigos_postales, nombres_estados):
        df.loc[(df['cod.postal'].notnull()) & (df['cod.postal'].str.contains(estado, na=False)), 'estado'] = estado
        
        
    # Terminada la normalizacion solo quedaron muy pocas filas donde no se puede saber su estado. Se eliminaran.
    df = df.dropna(subset=['estado'])
    df = df.drop(columns=['nombre'])

    # Agregamos un identificador unico a cada estado para que se conecte con la tabla principal g_sitios.
    df['id_estado'] = df['estado'].factorize()[0] + 1
                
    # Selecciona solo las columnas necesarias
    df_expanded = df[['gmap_id', 'address', 'latitude', 'longitude', 'direccion', 'ciudad', 'cod_postal', 'estado']]

    # Carga el DataFrame resultante a BigQuery
    # Verifica si la tabla ya existe y tiene datos
    table = client.get_table(table_id)
    if table.num_rows == 0:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    else:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

    # Cargar los datos a BigQuery
    client.load_table_from_dataframe(df_expanded, table_id, job_config=job_config).result()
    print(f"Datos del archivo {archivo} cargados exitosamente en BigQuery.")