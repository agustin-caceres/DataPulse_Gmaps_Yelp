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

1. **Preferencias del Usuario**: El usuario puede ingresar ciertas preferencias a definir mediante un conjunto de filtros interactivos ubicados en la barra lateral.
   
2. **Recomendaciones Personalizadas**: La aplicación muestra recomendaciones basadas en las preferencias ingresadas por el usuario, generando información relevante de cada restaurante, como su calificación, tipo de cocina, ubicación y más (a definir aun)

## 🔄 Flujo del Pipeline
**Input de Preferencias del Usuario:**

  - El usuario selecciona sus preferencias en la barra lateral ``(filters.py)``. Estas preferencias son procesadas en el archivo ``components/filters.py`` y se devuelven en un diccionario.
  - Estas preferencias se enviarán luego como parámetros al modelo de recomendación.

**Generación de Recomendaciones:**
- Con las preferencias del usuario, ``display_recommendations`` llama a ``generate_dummy_recommendations`` en ``utils/helpers.py`` para filtrar los resultados en base a los criterios seleccionados.
- En esta fase, los datos dummy se utilizarán para mostrar la funcionalidad general, pero posteriormente, se integrará con el modelo de Machine Learning real.

**Visualización de Resultados:**
- Las recomendaciones filtradas se presentan en un formato visual (disposición horizontal o vertical según preferencia del producto final) para una experiencia de usuario optimizada.
- Las recomendaciones se despliegan usando ``display_recommendations``, donde se podrá aplicar una disposición horizontal o vertical dependiendo de la preferencia final.

**Aplicación del Tema (Opcional al Final):**
- La paleta de colores predeterminada de Streamlit estará desactivada durante el desarrollo. Cuando la estructura final esté lista, se aplicará el tema definido en themes.py.

## 🎨 Paleta de Colores

La aplicación utiliza una paleta de colores basada en el logo de la empresa ficticia:

- Fondo: `#f8efd9`
- Primario: `#79b4b7`
- Secundario: `#344643`

Esta paleta se aplica mediante el archivo `styles/themes.py`, que centraliza los colores para una consistencia visual en toda la aplicación.

## 🛠️ Próximos Pasos

Este README se actualizará a medida que se agreguen nuevas funcionalidades a la aplicación. Actualmente, la aplicación está en su etapa inicial y se seguirán implementando mejoras.

**Las próximas implementaciones incluyen:**

- Integración con el modelo de recomendación de Machine Learning para generar resultados basados en datos históricos.
- Conexión con una API a través de FastAPI para que el modelo sea accesible de forma remota.

---

**Nota**: Este README está destinado específicamente para la sección de la aplicación desarrollada en **Streamlit** y no reemplaza al README general del proyecto.
