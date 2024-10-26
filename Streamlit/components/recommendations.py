# Archivo: components/recommendations.py

import streamlit as st
from utils.helpers import generate_dummy_recommendations

def display_recommendations(user_preferences):
    """
    Módulo para mostrar las recomendaciones generadas a partir de las preferencias del usuario.
    
    Args:
        user_preferences (dict): Preferencias del usuario
    """
    recommendations = generate_dummy_recommendations(user_preferences)
    if recommendations:
        for rec in recommendations:
            with st.container():
                st.subheader(f"{rec['nombre']} {get_food_icon(rec['tipo_de_comida'])}")
                st.write(f"📍 Ubicación: {rec['ubicacion']}")
                st.write(f"🍽️ Tipo de comida: {rec['tipo_de_comida']}")
                st.write(f"⭐ Calificación: {rec['calificacion']} / 5")
                st.markdown("<hr style='border-top: 2px solid #79b4b7;'>", unsafe_allow_html=True)  # Línea divisoria
    else:
        st.write("No se encontraron recomendaciones para las preferencias seleccionadas.")

def get_food_icon(food_type):
    """
    Función para asignar un emoji según el tipo de comida.
    """
    icons = {
        "Italiana": "🍕",
        "China": "🥡",
        "Mexicana": "🌮",
        "Japonesa": "🍣",
        "Todos": "🍽️"
    }
    return icons.get(food_type, "🍽️")