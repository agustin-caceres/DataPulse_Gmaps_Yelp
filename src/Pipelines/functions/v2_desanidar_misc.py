import os
import json
from google.cloud import storage

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

    archivo_desanidado = archivo.replace(".json", "_desanidado.json")
    with open(archivo_desanidado, 'w') as f:
        for registro in desanidado_datos:
            f.write(json.dumps(registro) + '\n')

    bucket_dest = storage_client.bucket(bucket_destino)
    new_blob = bucket_dest.blob(archivo_desanidado)
    new_blob.upload_from_filename(archivo_desanidado)
    print(f"Archivo desanidado {archivo_desanidado} cargado en el bucket {bucket_destino}.")

    blob.delete()
    os.remove(archivo_desanidado)
    print(f"Archivo {archivo} movido y desanidado de {bucket_source} a {bucket_destino}.")
