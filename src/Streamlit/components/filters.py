# Archivo: components/filters.py

import streamlit as st

def user_preferences_filter():
    """
    Módulo para recolectar las preferencias del usuario mediante filtros interactivos.
    
    Returns:
        dict: Diccionario con las preferencias del usuario
    """
    preferences = {}
    # Filtro de ubicación con los estados seleccionados
    preferences['ubicacion'] = st.sidebar.selectbox(
        "📍 Ubicación", 
        ["Nueva York", "Florida", "Texas", "Nuevo México", "Pensilvania"]
    )
    preferences['tipo_de_comida'] = st.sidebar.selectbox("🍽️ Tipo de comida", ["Todos", "Italiana", "China", "Mexicana", "Japonesa"])
    
    # Filtro de atributos específicos
    preferences['atributos'] = st.sidebar.multiselect(
        "🔍 Atributos específicos",
        ["Estacionamiento", "Espacios al aire libre", "Para niños", "Para adultos", "Acceso para discapacitados"]
    )
    
    preferences['calificacion_minima'] = st.sidebar.slider("⭐ Calificación", 1, 5)
    
    return preferences