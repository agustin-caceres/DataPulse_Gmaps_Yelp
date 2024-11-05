from google.cloud import bigquery
import pandas as pd

def crear_tabla_temporal(project_id: str, dataset: str, temp_table: str, schema: list) -> None:
    """
    Crea una tabla temporal en BigQuery con el esquema especificado.

    Args:
        project_id (str): ID del proyecto de GCP.
        dataset (str): Nombre del dataset en BigQuery.
        temp_table (str): Nombre de la tabla temporal a crear.
        schema (list): Lista de campos con el esquema de la tabla.

    Returns:
        None
    """
    try:
        client = bigquery.Client(project=project_id)
        table_id = f"{project_id}.{dataset}.{temp_table}"
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table, exists_ok=True)
        print(f"Tabla temporal '{table_id}' creada o ya existente.")
    except Exception as e:
        print(f"Error al crear la tabla temporal {temp_table}: {e}")

def cargar_dataframe_a_bigquery(df: pd.DataFrame, project_id: str, dataset: str, table_name: str) -> None:
    """
    Carga un DataFrame en una tabla específica de BigQuery.

    Args:
        df (pd.DataFrame): DataFrame a cargar en BigQuery.
        project_id (str): ID del proyecto de GCP.
        dataset (str): Nombre del dataset en BigQuery.
        table_name (str): Nombre de la tabla donde se cargará el DataFrame.

    Returns:
        None
    """
    try:
        if df.empty:
            print("El DataFrame está vacío. No se cargarán datos en BigQuery.")
            return

        client = bigquery.Client(project=project_id)
        table_id = f"{project_id}.{dataset}.{table_name}"
        job = client.load_table_from_dataframe(df, table_id)
        job.result()  # Espera a que la carga se complete
        print(f"Datos cargados en la tabla {table_id}")
    except Exception as e:
        print(f"Error al cargar los datos en la tabla {table_name}: {e}")
