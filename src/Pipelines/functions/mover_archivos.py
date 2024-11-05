from google.cloud import storage

def mover_archivos(bucket_source: str, bucket_destino: str, subcarpeta: str) -> None:
    """
    Mueve todos los archivos de una subcarpeta específica en un bucket de Google Cloud Storage
    a otro bucket y elimina los archivos del bucket de origen después de transferirlos.

    Args:
        bucket_source (str): Nombre del bucket de origen desde el cual se copiarán los archivos.
        bucket_destino (str): Nombre del bucket de destino al cual se copiarán los archivos.
        subcarpeta (str): Ruta de la subcarpeta dentro del bucket de origen que contiene los archivos a mover.
        
    Returns:
        None
    """
    # Crear cliente de Google Cloud Storage
    client = storage.Client()
    
    # Acceder al bucket de origen y al bucket de destino
    bucket_src = client.bucket(bucket_source)
    bucket_dest = client.bucket(bucket_destino)

    # Listar todos los blobs que estén en la subcarpeta especificada del bucket de origen
    blobs = bucket_src.list_blobs(prefix=subcarpeta)

    for blob in blobs:
        # Crear un nuevo blob en el bucket destino con el mismo nombre del archivo original
        new_blob = bucket_dest.blob(blob.name)
        
        # Copiar el archivo al bucket destino
        new_blob.rewrite(blob)
        
        # Eliminar el archivo original del bucket de origen para completar el movimiento
        blob.delete()
        
        print(f"Archivo {blob.name} movido a {bucket_destino} y eliminado de {bucket_source}.")
