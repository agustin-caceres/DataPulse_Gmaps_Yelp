import streamlit as st
import requests
from components.filters import user_preferences_filter
from components.recommendations import display_recommendations
from styles.themes import COLORS, apply_theme

# Configuración básica de la página
st.set_page_config(page_title="Recomendador de Restaurantes", layout="wide", initial_sidebar_state="expanded")

# Aplicación de la paleta de colores (opcional)
# apply_theme()

# Función para conectar con el backend
def get_recommendations(preferences):
    """
    Envía las preferencias del usuario al backend y obtiene recomendaciones.
    
    Args:
        preferences (dict): Diccionario con las preferencias del usuario.

    Returns:
        list: Lista de recomendaciones o None si ocurre un error.
    """
    url = "http://127.0.0.1:8000/get_recomendations"  # CAMBIAR URL ADECUADO

    try:
        response = requests.get(url, json=preferences)
        response.raise_for_status()  # Verifica si hubo un error HTTP
        return response.json()  # Devuelve el JSON de recomendaciones
    except requests.exceptions.RequestException as e:
        st.error(f"Error al conectarse con el backend: {e}")
        return None

# Encabezado principal
st.title("🍽️ Recomendador de Restaurantes Personalizado")
st.markdown("""
Esta aplicación proporciona recomendaciones personalizadas de restaurantes basadas en tus preferencias. 
Explora nuevas opciones y descubre los mejores lugares según tus gustos.
""")

# Aplicación modular con secciones bien definidas
def main():
    # Sección de filtros para las preferencias del usuario
    st.sidebar.header("Preferencias")
    user_preferences = user_preferences_filter()

    # Botón para restablecer preferencias
    if st.sidebar.button("Restablecer preferencias"):
        st.rerun()  # Reinicia la app

    # Sección para mostrar las recomendaciones
    st.header("🔍 Recomendaciones Personalizadas")
    if user_preferences:
        with st.spinner("Generando recomendaciones..."):
            recommendations = get_recommendations(user_preferences)
            if recommendations:
                display_recommendations(recommendations)
            else:
                st.error("No se pudieron obtener recomendaciones. Intenta más tarde.")
    else:
        st.write("Por favor, selecciona tus preferencias para obtener recomendaciones.")

    # Información adicional sobre la aplicación
    with st.expander("¿Cómo funciona este recomendador de restaurantes?"):
        st.write("Esta aplicación utiliza un modelo de recomendación que se basa en análisis de reseñas, información de ubicación, y otros atributos clave para ofrecer recomendaciones personalizadas y precisas.")

if __name__ == "__main__":
    main()
