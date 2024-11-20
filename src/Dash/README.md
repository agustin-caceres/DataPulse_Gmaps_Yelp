#  **Restaurant Recommendation App**

Esta aplicación en Dash ofrece un sistema de recomendación de restaurantes basado en preferencias del usuario y datos específicos de ubicación y características del negocio. Además, incluye integración con Google Maps para visualizar las ubicaciones sugeridas.

## 🌐 Acceso a la Aplicación

La aplicación está desplegada en línea y puedes acceder a ella a través del siguiente enlace:

[🚀 Acceder a la aplicación](https://restaurant-recommendations.onrender.com)

## ✨ **Características**
- 🎯 Filtros dinámicos para usuario, ubicación, preferencias y distancia.
- 🗂️ Visualización clara de resultados en tarjetas con horarios y enlaces a Google Maps.
- 🌌 Estilo moderno en **modo oscuro** con una interfaz responsiva.
- 🌐 Acceso en línea desde cualquier navegador.

## 📂 **Estructura del Proyecto**
- **main.py**: Archivo principal de la aplicación  
- **assets/**: Contiene archivos estáticos como imágenes, logos, etc.
    - ``styles.css:`` Estilo CSS personalizado 
- **paginas/** : Contiene las páginas de la aplicación
    - ``page_1.py:`` Layout principal de la aplicación 
- **functions/** 
    - ``api_request.py:`` Reliza las solicitudes al backend de FastAPI 
    - ``config.py:`` Configuración de la app (ciudades, estados, etc.)



## 🚀 **Cómo Usar**
1. 🌐 Accede a la aplicación en línea en la URL pública.
2. 🎯 Configura los filtros en la barra lateral:
   - **Usuario:** Selecciona un ID de usuario o si eres Usuario Nuevo.
   - **Distancia y Ubicación:** Define un rango en kilómetros y un estado/ciudad.
   - **Preferencias:** Escoge características y categorías según preferencias, también puedes omitirlas.
3. ✨ Haz clic en **"Obtener Recomendaciones"** y navega por los resultados.
4. 📍 Usa el botón de **"Ver en Google Maps"** para explorar las ubicaciones.

<div align="center">
<img src="https://www.greghilston.com/post/how-to-use-plotly-plotly-express-and-dash-with-jupyterlab/featured-image.png" alt="Dash Logo" width="150">
</div>


