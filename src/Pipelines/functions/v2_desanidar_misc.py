import os
import json
from google.cloud import storage
import tempfile

def desanidar_y_mover_archivo(bucket_source: str, archivo: str, bucket_destino: str) -> None:
    """
    Desanida el contenido de 'MISC' en un archivo JSON y luego lo mueve al bucket de destino.

    Args:
        bucket_source (str): Nombre del bucket de origen en Google Cloud Storage.
        archivo (str): Nombre del archivo JSON a procesar.
        bucket_destino (str): Nombre del bucket de destino en Google Cloud Storage.
    """
    storage_client = storage.Client()
    bucket_src = storage_client.bucket(bucket_source)
    blob = bucket_src.blob(archivo)
    
    contenido = blob.download_as_text()
    desanidado_datos = []

    for linea in contenido.splitlines():
        try:
            contenido_json = json.loads(linea)
        except json.JSONDecodeError as e:
            print(f"Error de decodificación JSON en la línea: {e}")
            continue

        gmap_id = contenido_json.get('gmap_id')
        misc = contenido_json.get('MISC')

        if gmap_id and misc:
            desanidado_miscs = [f"{key}: {value}" for key, value in misc.items() if value is not None]
            for item in desanidado_miscs:
                desanidado_datos.append({"gmap_id": gmap_id, "misc": item})

    # Usar un directorio temporal
    temp_dir = tempfile.gettempdir()
    archivo_desanidado = os.path.join(temp_dir, archivo.replace(".json", "_desanidado.json"))
    
    # Escribir el archivo desanidado
    with open(archivo_desanidado, 'w') as f:
        for registro in desanidado_datos:
            f.write(json.dumps(registro) + '\n')

    # Subir el archivo desanidado al bucket de destino
    bucket_dest = storage_client.bucket(bucket_destino)
    new_blob = bucket_dest.blob(os.path.basename(archivo_desanidado))  # Solo el nombre del archivo
    new_blob.upload_from_filename(archivo_desanidado)
    print(f"Archivo desanidado {archivo_desanidado} cargado en el bucket {bucket_destino}.")

    # Eliminar el archivo original
    blob.delete()
    os.remove(archivo_desanidado)  # Eliminar el archivo temporal
    print(f"Archivo {archivo} movido y desanidado de {bucket_source} a {bucket_destino}.")

