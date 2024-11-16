import streamlit as st

def user_preferences_filter():
    """
    Módulo para recolectar las preferencias del usuario mediante filtros interactivos.
    
    Returns:
        dict: Diccionario con las preferencias del usuario
    """
    preferences = {}
    
    # Estado (state)
    preferences['state'] = st.sidebar.selectbox(
        "📍 Ubicación",
        ["Nueva York", "Florida", "Texas", "Nuevo México", "Pensilvania"]
    )
    
    # Tipo de negocio (category)
    preferences['category'] = st.sidebar.selectbox(
        "🍽️ Tipo de comida",
        ["Todos", "Italiana", "China", "Mexicana", "Japonesa"]
    )
    
    # ID del usuario
    preferences['user_id_str'] = st.sidebar.text_input("🔑 ID de Usuario", value="user123")
    
    # Número de recomendaciones (top_n)
    preferences['top_n'] = st.sidebar.slider("🔢 Número de recomendaciones", 1, 5, value=3)
    
    return preferences
