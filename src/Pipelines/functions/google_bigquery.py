from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import logging
import json

################################################################

def crear_tablas_bigquery(project_id: str, dataset: str) -> None:
    """
    Crea múltiples tablas en el dataset de BigQuery si no existen.
    
    Args:
    -------
    project_id : str
        ID del proyecto en Google Cloud Platform.
    dataset : str
        Nombre del dataset en BigQuery.
    """
    client = bigquery.Client()

    # Diccionario con las definiciones de tablas: nombre de la tabla y consulta SQL de creación
    tablas = {
        "miscelaneos": f"""
            CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.miscelaneos` (
                gmap_id STRING,
                misc STRING
            )
        """,
        "relative_results": f"""
            CREATE TABLE IF NOT EXISTS `{project_id}.{dataset}.relative_results` (
                gmap_id STRING,
                relative_results STRING
            )
        """
        # Agrega más tablas aquí si es necesario
    }
    
    # Ejecuta la consulta de creación para cada tabla
    for nombre_tabla, create_query in tablas.items():
        client.query(create_query).result()
        logging.info(f"Tabla '{nombre_tabla}' creada o ya existente.")
        
##############################################################################################

def eliminar_tablas_temporales(project_id: str, dataset: str) -> None:
    """
    Elimina las tablas temporales en BigQuery.
    
    Args:
    -------
    project_id : str
        ID del proyecto en Google Cloud Platform.
    dataset : str
        Nombre del dataset en BigQuery.
    """
    client = bigquery.Client()

    # Lista de tablas temporales a eliminar
    tablas_temporales = [
        f"{project_id}.{dataset}.temp_miscelaneos",
        f"{project_id}.{dataset}.miscelaneos",
        f"{project_id}.{dataset}.relative_results"
    ]
    
    # Bucle para eliminar cada tabla temporal
    for table_id in tablas_temporales:
        drop_query = f"DROP TABLE IF EXISTS `{table_id}`"
        client.query(drop_query).result()
        logging.info(f"Tabla '{table_id}' eliminada con éxito.")
