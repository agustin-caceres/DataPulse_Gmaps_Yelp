from google.cloud import bigquery

def desanidar_misc(project_id: str, dataset: str, temp_table: str, final_table: str) -> None:
    """
    Ejecuta una consulta SQL en BigQuery para desanidar la columna `MISC` desde la tabla temporal
    y carga los resultados en la tabla final.

    Args:
    -------
    project_id : str
        ID del proyecto en Google Cloud Platform.
    dataset : str
        Nombre del dataset en BigQuery.
    temp_table : str
        Nombre de la tabla temporal en BigQuery con los datos crudos.
    final_table : str
        Nombre de la tabla final en BigQuery donde se guardarán los datos desanidados.
    """

    client = bigquery.Client()

    # Consulta SQL para desanidar `MISC`
    query = f"""
    INSERT INTO `{project_id}.{dataset}.{final_table}` (gmap_id, misc)
    SELECT
        gmap_id,
        CONCAT(key, ": ", value) AS misc
    FROM
        `{project_id}.{dataset}.{temp_table}`,
        UNNEST(REGEXP_EXTRACT_ALL(TO_JSON_STRING(MISC), r'"(\w+)": ?"?(.*?)"?,"?')) AS key_value
    WHERE
        key_value IS NOT NULL
    """

    # Ejecuta la consulta
    query_job = client.query(query)
    query_job.result()  # Espera a que termine la consulta
    print("Desanidado y carga en la tabla final completados exitosamente.")
