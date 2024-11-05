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
    try:
        client = bigquery.Client(project=project_id)
        
        # Verificar que la tabla temporal existe antes de realizar la carga
        temp_table_id = f"{project_id}.{dataset}.{temp_table}"
        table_ref = client.get_table(temp_table_id)  # Lanza una excepción si la tabla no existe

        # Consulta SQL para crear la tabla final a partir de la temporal.
        # CREATE OR REPLACE TABLE asegura que la tabla final se sobrescribirá cada vez que se ejecute.
        query = f"""
        CREATE OR REPLACE TABLE `{project_id}.{dataset}.{final_table}` AS
        SELECT * FROM `{temp_table_id}`
        """
        
        # Ejecuta la consulta para cargar los datos en la tabla final
        client.query(query).result()
        print(f"Datos cargados en la tabla final `{final_table}`.")

    except Exception as e:
        # Si ocurre un error, imprime un mensaje detallado para depuración
        print(f"Error al cargar datos en la tabla final `{final_table}`: {e}")

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
    try:
        client = bigquery.Client(project=project_id)
        table_id = f"{project_id}.{dataset}.{temp_table}"
        
        # Elimina la tabla temporal
        # not_found_ok=True previene errores si la tabla ya fue eliminada
        client.delete_table(table_id, not_found_ok=True)
        print(f"Tabla temporal `{temp_table}` eliminada.")

    except Exception as e:
        # Si ocurre un error, imprime un mensaje detallado para depuración
        print(f"Error al eliminar la tabla temporal `{temp_table}`: {e}")
