import streamlit as st

def display_recommendations(recommendations):
    """
    Módulo para mostrar las recomendaciones generadas por el backend.
    
    Args:
        recommendations (list): Lista de recomendaciones generadas por el backend.
    """
    if recommendations:
        for i in range(0, len(recommendations), 3):  # Agrupar de tres en tres
            cols = st.columns(3)
            for col, rec in zip(cols, recommendations[i:i+3]):
                with col:
                    st.subheader(f"{rec['name']} {get_food_icon(rec.get('category', 'Todos'))}")
                    st.write(f"📍 Ubicación: {rec['city']}, {rec['state']}")
                    st.write(f"📦 Dirección: {rec['address']}")
                    st.write(f"🌍 Coordenadas: {rec['latitude']}, {rec['longitude']}")
                    st.markdown("<hr style='border-top: 2px solid #79b4b7;'>", unsafe_allow_html=True)
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
