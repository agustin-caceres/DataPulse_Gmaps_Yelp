import requests

# URL base del backend
BASE_URL = "https://recomendador-backend-648324574770.us-central1.run.app"

def get_recommendations(km, estado, ciudad, usuario, caracteristicas, categorias):
    """
    Realiza una solicitud POST al endpoint de recomendaciones del backend.

    Args:
        km (int): Distancia en kilómetros.
        estado (str): Estado donde buscar las recomendaciones.
        ciudad (str): Ciudad específica.
        usuario (str): ID del usuario.
        caracteristicas (list): Lista de características seleccionadas.
        categorias (list): Lista de categorías seleccionadas.

    Returns:
        dict: Respuesta del backend con las recomendaciones, o None si ocurre un error.
    """
    url = f"{BASE_URL}/get_recomendations/"
    payload = {
        "km": km,
        "estado": estado,
        "ciudad": ciudad,
        "usuario": usuario,
        "caracteristicas": caracteristicas,
        "categorias": categorias
    }

    try:
        # Enviar la solicitud al backend
        response = requests.post(url, json=payload)
        response.raise_for_status()  # Lanza un error si la respuesta no es 200
        return response.json()  # Devuelve la respuesta en formato JSON
    except requests.exceptions.RequestException as e:
        print(f"Error al conectar con el backend: {e}")
        return None