import requests
import json
import re
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery
import logging

# Configuración del logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clean_text(text):
    """
    Elimina emojis y caracteres especiales de un texto.
    """
    return re.sub(r'[^\w\s.,!?]+', '', text)

def extraer_reviews_google_places(api_key, businesses, bucket_name, output_file):
    """
    Extrae las reseñas más recientes de negocios desde Google Places API y las guarda en un archivo JSON en GCS.

    Args:
        api_key (str): Clave de la API de Google Places.
        businesses (list): Lista de negocios con nombre, ubicación y radio de búsqueda.
        bucket_name (str): Nombre del bucket en Google Cloud Storage.
        output_file (str): Nombre del archivo JSON donde se guardarán los datos en GCS.
    """
    all_reviews = []

    # Limitar a un máximo de 5 consultas
    for i, business in enumerate(businesses):
        if i >= 5:
            break
        
        search_url = f"https://maps.googleapis.com/maps/api/place/findplacefromtext/json?input={business['name']}&inputtype=textquery&fields=place_id&locationbias=circle:{business['radius']}@{business['location']}&key={api_key}"
        search_response = requests.get(search_url)
        search_data = search_response.json()
        
        if search_data['candidates']:
            place_id = search_data['candidates'][0]['place_id']
            details_url = f"https://maps.googleapis.com/maps/api/place/details/json?place_id={place_id}&key={api_key}"
            details_response = requests.get(details_url)
            details_data = details_response.json()
            
            if 'result' in details_data and 'reviews' in details_data['result']:
                reviews = details_data['result']['reviews']
                reviews.sort(key=lambda x: x['time'], reverse=True)
                most_recent_review = reviews[0]
                
                cleaned_review_text = clean_text(most_recent_review['text'])
                review_timestamp = most_recent_review['time']
                review_date = datetime.utcfromtimestamp(review_timestamp).strftime('%Y-%m-%d')

                business_info = {
                    "place_id": place_id,
                    "business_name": business['name'],
                    "author": most_recent_review['author_name'],
                    "review": cleaned_review_text,
                    "date": review_date
                }
                
                all_reviews.append(business_info)
            else:
                logger.info(f"No se encontraron reseñas para el negocio '{business['name']}'")
        else:
            logger.info(f"No se encontró el negocio '{business['name']}'")
    
    # Guardar los datos en un archivo JSON
    json_data = json.dumps(all_reviews, indent=4)
    
    # Subir el archivo JSON a Google Cloud Storage
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(output_file)
    blob.upload_from_string(json_data, content_type='application/json')
    
    logger.info(f"Archivo '{output_file}' guardado exitosamente en el bucket '{bucket_name}'.")

# Función para cargar el archivo JSON de GCS a BigQuery
def cargar_a_bigquery(bucket_name, output_file, dataset_name, table_name):
    """
    Carga un archivo JSON desde Google Cloud Storage (GCS) a una tabla en BigQuery.
    
    La función toma un archivo JSON almacenado en un bucket de Google Cloud Storage y lo carga en una tabla de BigQuery.
    Si la tabla ya contiene datos, estos se sobrescriben (WRITE_TRUNCATE).
    
    Args:
        bucket_name (str): El nombre del bucket de Google Cloud Storage donde se encuentra el archivo.
        output_file (str): El nombre del archivo JSON en el bucket de GCS.
        dataset_name (str): El nombre del dataset en BigQuery donde se encuentra la tabla destino.
        table_name (str): El nombre de la tabla en BigQuery donde se cargarán los datos.
     """

    client = bigquery.Client()
    table_ref = client.dataset(dataset_name).table(table_name)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    uri = f"gs://{bucket_name}/{output_file}"
    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()  # Espera hasta que la carga esté completa

    logger.info(f"Datos de {output_file} cargados exitosamente en BigQuery en {dataset_name}.{table_name}")