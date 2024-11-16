# 🍽️ Recomendador de Restaurantes - Streamlit App

Este directorio contiene la implementación de la aplicación **Recomendador de Restaurantes** desarrollada con **Streamlit**. La aplicación permite a los usuarios obtener recomendaciones personalizadas de restaurantes en función de sus preferencias.

## 🌐 Acceso a la Aplicación

La aplicación está desplegada en línea y puedes acceder a ella a través del siguiente enlace:

[🚀 Acceder a la aplicación](https://recomendador-restaurantes.streamlit.app/)

## 🗂️ Estructura de Directorios

- **assets/**: Contiene archivos estáticos como imágenes, logos, etc.
- **components/**: Módulos reutilizables que incluyen la funcionalidad principal de la aplicación.
  - `filters.py`: Módulo que permite al usuario ingresar sus preferencias.
  - `recommendations.py`: Módulo encargado de mostrar las recomendaciones generadas según las preferencias del usuario.
- **styles/**: Define la paleta de colores y los estilos de la aplicación.
  - `themes.py`: Contiene la paleta de colores y funciones para aplicar el tema a la aplicación.
- **utils/**: Funciones auxiliares y utilidades generales para el proyecto.
  - `helpers.py`: Funciones para generar recomendaciones ficticias y otras utilidades.
- **app.py**: Archivo principal para ejecutar la aplicación.

## 🎯 Funcionalidades Principales

1. **Preferencias del Usuario**: El usuario puede ingresar ciertas preferencias a definir mediante un conjunto de filtros interactivos ubicados en la barra lateral.
   
2. **Recomendaciones Personalizadas**: La aplicación muestra recomendaciones basadas en las preferencias ingresadas por el usuario, generando información relevante de cada restaurante, como su calificación, tipo de cocina, ubicación y más (a definir aun)

## 🔄 Flujo del Pipeline
**Input de Preferencias del Usuario:**

  - El usuario selecciona sus preferencias en la barra lateral ``(filters.py)``. Estas preferencias son procesadas en el archivo ``components/filters.py`` y se devuelven en un diccionario.
  - Estas preferencias se procesan y se envían al backend en formato JSON.

**Procesamiento en el Backend:**
- El backend utiliza un modelo de ML para calcular recomendaciones basadas en las preferencias enviadas.

**Visualización de Resultados:**
- Las recomendaciones filtradas se presentan en un formato visual para una experiencia de usuario optimizada.

**Aplicación del Tema (Opcional al Final):**
- La paleta de colores predeterminada de Streamlit estará desactivada durante el desarrollo. Cuando la estructura final esté lista, se aplicará el tema definido en themes.py.

## 🎨 Paleta de Colores

La aplicación utiliza una paleta de colores basada en el logo de la empresa ficticia:

- Fondo: `#f8efd9`
- Primario: `#79b4b7`
- Secundario: `#344643`

Esta paleta se aplica mediante el archivo `styles/themes.py`, que centraliza los colores para una consistencia visual en toda la aplicación.

## 🛠️ Próximos Pasos

1. Finalizar la integración con el backend:
  - Enviar datos reales al endpoint /get_recomendations en FastAPI.
  - Mostrar resultados dinámicos en la app.
2. Optimización del Diseño:
  - Aplicar la paleta de colores y ajustar el diseño para una mejor experiencia.

<div align="center">
<img src="https://streamlit.io/images/brand/streamlit-logo-primary-colormark-darktext.png" alt="Streamlit Logo" width="150">
</div>
