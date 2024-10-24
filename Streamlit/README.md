# 🍽️ Recomendador de Restaurantes - Streamlit App

Este directorio contiene la implementación de la aplicación **Recomendador de Restaurantes** desarrollada con **Streamlit**. La aplicación permite a los usuarios obtener recomendaciones personalizadas de restaurantes en función de sus preferencias.

## 🌐 Acceso a la Aplicación

La aplicación está desplegada en línea y puedes acceder a ella a través del siguiente enlace:

[🚀 Acceder a la aplicación](URL_DEL_DEPLOY)

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

1. **Preferencias del Usuario**: El usuario puede ingresar la ubicación, tipo de comida, y rango de precios de su preferencia mediante un conjunto de filtros interactivos ubicados en la barra lateral.
   
2. **Recomendaciones Personalizadas**: La aplicación muestra recomendaciones basadas en las preferencias ingresadas por el usuario, generando información relevante de cada restaurante, como su calificación, tipo de cocina, ubicación y más.

## 🎨 Paleta de Colores

La aplicación utiliza una paleta de colores basada en el logo de la empresa ficticia:

- Fondo: `#f8efd9`
- Primario: `#79b4b7`
- Secundario: `#344643`

Esta paleta se aplica mediante el archivo `styles/themes.py`, que centraliza los colores para una consistencia visual en toda la aplicación.

## 🛠️ Próximos Pasos

Este README se actualizará a medida que se agreguen nuevas funcionalidades a la aplicación. Actualmente, la aplicación está en su etapa inicial y se seguirán implementando mejoras, incluyendo la integración con el modelo de recomendación y la API con **FastAPI**.

---

**Nota**: Este README está destinado específicamente para la sección de la aplicación desarrollada en **Streamlit** y no reemplaza al README general del proyecto.
