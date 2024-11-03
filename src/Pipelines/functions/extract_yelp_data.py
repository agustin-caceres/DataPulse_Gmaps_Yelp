from google.cloud import storage
import json

def flatten_data(nested_data, parent_key='', sep='_', selected_columns=None):
    """
    Desanida estructuras complejas de un JSON (listas y diccionarios anidados),
    permitiendo seleccionar columnas específicas.

    Parámetros:
    -----------
    nested_data : dict
        Diccionario con datos anidados.
    parent_key : str
        Clave base para los datos anidados (se usa para crear una jerarquía en la clave).
    sep : str
        Separador para claves anidadas (por ejemplo, 'clave_subclave').
    selected_columns : list, opcional
        Lista de columnas que deseas incluir en el resultado final. Si es None, 
        se incluyen todas las columnas.

    Retorna:
    --------
    dict
        Diccionario aplanado con solo las columnas seleccionadas.
    """
    items = []
    if isinstance(nested_data, dict):
        for key, value in nested_data.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            if selected_columns and new_key not in selected_columns:
                continue  # Salta esta clave si no está en las columnas seleccionadas
            if isinstance(value, dict):
                items.extend(flatten_data(value, new_key, sep=sep, selected_columns=selected_columns).items())
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    items.extend(flatten_data(item, f"{new_key}_{i}", sep=sep, selected_columns=selected_columns).items())
            else:
                items.append((new_key, value))
    elif isinstance(nested_data, list):
        for i, item in enumerate(nested_data):
            items.extend(flatten_data(item, f"{parent_key}_{i}", sep=sep, selected_columns=selected_columns).items())
    else:
        if not selected_columns or parent_key in selected_columns:
            items.append((parent_key, nested_data))
    return dict(items)


from google.cloud import storage
import json

def extract_data(bucket_name: str, nombre_archivo: str, selected_columns=None) -> list:
    """
    Extrae datos desde un archivo JSON en Cloud Storage y desanida estructuras complejas,
    seleccionando solo las columnas indicadas.

    Parámetros:
    -----------
    bucket_name : str
        Nombre del bucket en Google Cloud Storage.
    nombre_archivo : str
        Nombre del archivo JSON a procesar.
    selected_columns : list, opcional
        Lista de columnas que deseas incluir en el resultado final. Si es None, 
        se incluyen todas las columnas.

    Retorna:
    --------
    list
        Lista de registros (diccionarios) aplanados con solo las columnas seleccionadas.
    """
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(nombre_archivo)

    # Descargar y cargar el archivo JSON
    data = json.loads(blob.download_as_text())

    # Manejar diferentes tipos de estructuras
    if isinstance(data, list):
        # Si es una lista de registros, aplicamos flatten a cada uno
        flat_data = [flatten_data(record, selected_columns=selected_columns) for record in data]
    elif isinstance(data, dict):
        # Si es un solo diccionario, aplicamos flatten directamente
        flat_data = [flatten_data(data, selected_columns=selected_columns)]
    else:
        raise ValueError("Estructura de datos no compatible. Se espera un dict o list.")

    return flat_data

