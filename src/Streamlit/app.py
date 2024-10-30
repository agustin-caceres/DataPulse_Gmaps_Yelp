# Archivo: app.py

import streamlit as st
from components.filters import user_preferences_filter
from components.recommendations import display_recommendations
from styles.themes import COLORS, apply_theme

# Configuración básica de la página
st.set_page_config(page_title="Recomendador de Restaurantes", layout="wide", initial_sidebar_state="expanded")

# Aplicación de la paleta de colores
# apply_theme()

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
        st.experimental_rerun()  # Reinicia la app

    # Sección para mostrar las recomendaciones
    st.header("🔍 Recomendaciones Personalizadas")
    if user_preferences:
        with st.spinner("Generando recomendaciones..."):
            display_recommendations(user_preferences)
    else:
        st.write("Por favor, selecciona tus preferencias para obtener recomendaciones.")

    # Información adicional sobre la aplicación
    with st.expander("¿Cómo funciona este recomendador de restaurantes?"):
        st.write("Esta aplicación utiliza un modelo de recomendación que se basa en análisis de reseñas, información de ubicación, y otros atributos clave para ofrecer recomendaciones personalizadas y precisas.")

if __name__ == "__main__":
    main()