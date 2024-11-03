from google.cloud import bigquery

def load_data(flat_data: list, project_id: str, dataset: str, table_name: str):
    """
    Carga los datos aplanados en una tabla de BigQuery.

    Parámetros:
    -----------
    flat_data : list
        Lista de registros (diccionarios) aplanados con 'gmap_id'.
    project_id : str
        ID del proyecto en Google Cloud Platform.
    dataset : str
        Nombre del dataset en BigQuery donde se encuentra la tabla de destino.
    table_name : str
        Nombre de la tabla en BigQuery donde se cargarán los datos.

    Retorna:
    --------
    None
    """
    client = bigquery.Client()
    table_id = f"{project_id}.{dataset}.{table_name}"

    # Configuración de carga
    errors = client.insert_rows_json(table_id, flat_data)
    if errors:
        print(f"Error al cargar los datos en BigQuery: {errors}")
    else:
        print("Datos cargados exitosamente en BigQuery.")
