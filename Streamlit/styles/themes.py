# Archivo: styles/themes.py

import streamlit as st

# Definir la paleta de colores de la empresa
COLORS = {
    "background": "#f8efd9",        # Fondo principal de la app
    "primary": "#79b4b7",           # Color primario (botones, encabezados destacados)
    "secondary": "#344643",         # Color secundario (sidebar y texto en fondo oscuro)
    "text_primary": "#344643",      # Color principal del texto
    "text_secondary": "#f8efd9",    # Color de texto en elementos con fondo oscuro
    "input_background": "#79b4b7",  # Fondo de los campos de entrada (sliders, inputs)
    "slider_track": "#344643",      # Color de la barra de track del slider
}

# Aplicación de la paleta de colores y estilo
def apply_theme():
    st.markdown(
        f"""
        <style>
            /* Fondo general de la aplicación */
            [data-testid="stAppViewContainer"] {{
                background-color: {COLORS['background']} !important;
                color: {COLORS['text_primary']} !important;
            }}
            
            /* Fondo y color de texto del sidebar */
            section[data-testid="stSidebar"] {{
                background-color: {COLORS['secondary']} !important;
                color: {COLORS['text_secondary']} !important;
            }}
            
            /* Encabezados */
            h1, h2, h3, h4, h5, h6 {{
                color: {COLORS['primary']} !important;
            }}
            
            /* Botones */
            div.stButton > button {{
                background-color: {COLORS['primary']} !important;
                color: {COLORS['text_secondary']} !important;
                border-radius: 5px;
                border: none;
            }}
            
            /* Campos de entrada (inputs y sliders) */
            input, select, textarea {{
                background-color: {COLORS['input_background']} !important;
                color: {COLORS['text_secondary']} !important;
                border-radius: 5px;
            }}

            /* Sliders */
            .stSlider > div > div {{
                background-color: {COLORS['input_background']} !important;
            }}
            .stSlider > div > div > div {{
                background: {COLORS['primary']} !important;  /* Color de fondo del slider */
            }}
            .stSlider > div > div > div:first-child {{
                background-color: {COLORS['slider_track']} !important;  /* Color de la barra de track */
            }}
            .stSlider > div > div > div > [role="slider"] {{
                background-color: {COLORS['secondary']} !important; /* Color del handle */
                border: 2px solid {COLORS['primary']} !important;
            }}
        </style>
        """,
        unsafe_allow_html=True
    )