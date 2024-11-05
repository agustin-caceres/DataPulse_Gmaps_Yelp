from google.cloud import bigquery

def cargar_en_tabla_final(project_id: str, dataset: str, temp_table: str, final_table: str) -> None:
    """
    Carga los datos transformados desde la tabla temporal en BigQuery a la tabla final `checkin_yelp`.
    
    Args:
        project_id (str): ID del proyecto de GCP.
        dataset (str): Nombre del dataset en BigQuery.
        temp_table (str): Nombre de la tabla temporal en BigQuery.
        final_table (str): Nombre de la tabla final en BigQuery.
    
    Returns:
        None
    """
    client = bigquery.Client(project=project_id)
    query = f"""
    CREATE OR REPLACE TABLE `{project_id}.{dataset}.{final_table}` AS
    SELECT * FROM `{project_id}.{dataset}.{temp_table}`
    """
    client.query(query).result()
    print(f"Datos cargados en la tabla final `{final_table}`.")


#######################################################################################


def eliminar_tabla_temporal(project_id: str, dataset: str, temp_table: str) -> None:
    """
    Elimina la tabla temporal en BigQuery.
    
    Args:
        project_id (str): ID del proyecto de GCP.
        dataset (str): Nombre del dataset en BigQuery.
        temp_table (str): Nombre de la tabla temporal en BigQuery.
    
    Returns:
        None
    """
    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset}.{temp_table}"
    client.delete_table(table_id, not_found_ok=True)
    print(f"Tabla temporal `{temp_table}` eliminada.")
