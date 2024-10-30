# Archivo: utils/helpers.py

def generate_dummy_recommendations(user_preferences):
    """
    Genera recomendaciones ficticias para demostración.
    
    Args:
        user_preferences (dict): Preferencias del usuario
    
    Returns:
        list: Lista de recomendaciones ficticias
    """
    # Aquí se pueden agregar más lógica para filtrar en base a las preferencias
    dummy_data = [
        {"nombre": "Pizza Place", "ubicacion": user_preferences['ubicacion'], "tipo_de_comida": "Italiana", "calificacion": 4.5},
        {"nombre": "Sushi Place", "ubicacion": user_preferences['ubicacion'], "tipo_de_comida": "Japonesa", "calificacion": 4.2},
        {"nombre": "Tacos y Más", "ubicacion": user_preferences['ubicacion'], "tipo_de_comida": "Mexicana", "calificacion": 4.0}
    ]
    
    filtered_recommendations = [rec for rec in dummy_data if rec['calificacion'] >= user_preferences['calificacion_minima']]
    return filtered_recommendations 