def generate_dummy_recommendations(user_preferences):
    """
    Genera recomendaciones ficticias para demostración.

    Args:
        user_preferences (dict): Preferencias del usuario
    
    Returns:
        list: Lista de recomendaciones ficticias
    """
    dummy_data = [
        {"name": "Pizza Place", "address": "123 Pizza St", "latitude": 34.052235, "longitude": -118.243683},
        {"name": "Sushi Place", "address": "456 Sushi Ave", "latitude": 34.052236, "longitude": -118.243684},
        {"name": "Tacos y Más", "address": "789 Taco Blvd", "latitude": 34.052237, "longitude": -118.243685}
    ]
    return dummy_data
