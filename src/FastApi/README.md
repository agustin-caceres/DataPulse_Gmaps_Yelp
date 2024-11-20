# DataPulse Analytics - FastAPI Backend

Este backend es parte de un sistema diseñado para ofrecer recomendaciones personalizadas de restaurantes, basándose en las preferencias de los usuarios y datos de ubicación. Utiliza modelos de ML y está diseñado para integrarse con una aplicación diseñada en Dash como frontend.

## 🛠️ Arquitectura del Sistema
El sistema tiene tres componentes principales:

1. **Backend (FastAPI):** Nuestro servidor que recibe solicitudes, aplica el modelo de ML y devuelve respuestas en JSON.
- Cómo funciona:
  - Define endpoints como /get_recomendations para recibir parámetros de entrada (estado, categoría, etc.).
  - Usa un modelo preentrenado para generar recomendaciones personalizadas.
  - Devuelve un JSON estructurado con las recomendaciones.

2. **Frontend (Dash)** Nuestra app que servira el modelo en una interfaz visual para que los usuarios interactúen con el sistema.
-  Cómo funciona:
    - Recoge los datos ingresados por los usuarios (estado, categoría, etc.).
    - Realiza solicitudes HTTP a la API del backend.
    - Muestra los resultados en tarjetas visuales.

3. **CI/CD y Despliegue**
- Pipeline de CI/CD:
  - Automatiza la construcción y despliegue del backend con GitHub Actions.
  - Cada cambio en la rama main genera automáticamente un nuevo despliegue en Google Cloud Run.
- Google Cloud Run:
  - Servicio que ejecuta el backend como un contenedor escalable en la nube.

## 🚀 Flujo de Datos
- **Entrada**: El usuario ingresa los siguientes datos en la app de Dash:
  - `km:` Distancia de las recomendaciones.
  - `estado:` Estado donde quiere las recomendaciones (por ejemplo, "California").
  - `ciudad:` Ciudades según el Estado seleccionado.
  - `usuario` Identificador del usuario.
  - `caracteristicas:` Características del negocio (por ejemplo, "Pet Friendly").
  - `categorias`: Categorías del negocio (por ejemplo, "Cómida Rápida").
  - ``top_n:`` Número de recomendaciones deseadas.
- **Procesamiento**
1. **Carga de recursos:**
    - Modelo de ML (model.pkl) para calcular las recomendaciones.
    - Matrices de características (user_features.pkl, item_features.pkl) y DataFrames auxiliares.
2. **Filtrado:**
    - Se filtran los negocios según el estado y la categoría proporcionados.
3. **Predicciones:**
    - El modelo calcula puntuaciones para los negocios relevantes.
    - Selecciona los top_n negocios con mayor puntuación.
4. **Respuesta:**
    - Devuelve un JSON con los detalles de cada recomendación (nombre, dirección, ciudad, coordenadas, etc.).
- **Salida**
```json
  {
    "recomendations": [
      {
      "negocio": "string",
      "direccion": "string",
      "ciudad": "string",
      "estado": "string",
      "lunes": "string",
      "martes": "string",
      "miercoles": "string",
      "jueves": "string",
      "viernes": "string",
      "sabado": "string",
      "domingo": "string",
      "distancia": 0,
      "latitud": 0,
      "longitud": 0
      }
    ]
  }
  ```
  

## 📁 Estructura del Proyecto

- **main.py:** Configuración principal de la API
- **routers/:** Endpoints de la API
  - router_get_recomendations.py: Endpoint para obtener recomendaciones
- **models/:** Modelos Pydantic para validaciones y respuestas
  - base_models.py: Definición de clases para solicitudes y respuestas
- **.github/workflows/:** Pipelines de CI/CD
  - deploy.yml: Workflow de despliegue en Google Cloud Run
- **Dockerfile:** Archivo para construir la imagen del contenedor
- **requirements.txt:** Dependencias del proyecto


## 🌐 Endpoints Disponibles


### **`GET /`**
- Responde con un mensaje de bienvenida y un enlace a la documentación Swagger.

### **`GET /get_recomendations`**
- **Descripción**: Devuelve recomendaciones personalizadas basadas en las preferencias del usuario.
- **Parámetros del payload**:
  ```json
  {
    "km": "27",
    "estado": "LA",
    "ciudad": "Riveridge",
    "usuario": "Usuario Nuevo",
    "caracteristicas": "Acepta Mascotas",
    "categorias": "Restaurantes Generales"
  }
  ```
- **Respuesta**:
```json
  {
    "recomendations": [
      {
      "negocio": "Crescent City Steak House",
      "direccion": "1001 N Broad St",
      "ciudad": "New Orleans",
      "estado": "LA",
      "lunes": "No Disponible",
      "martes": "16:0 - 21:0",
      "miercoles": "11:30 - 21:0",
      "jueves": "11:30 - 21:0",
      "viernes": "11:30 - 21:30",
      "sabado": "16:0 - 21:30",
      "domingo": "12:0 - 21:0",
      "distancia": 14.21,
      "latitud": 29.9733253,
      "longitud": -90.0810244
      }
    ]
  }
  ```


## 🧠 Modelo ML
El modelo de recomendaciones utiliza LightFM, ideal para:
- Recomendaciones basadas en interacciones (reseñas, puntuaciones, etc.).
- Incorporar datos adicionales mediante matrices de características.

**Proceso:**
1. Entrada:
    - ID del usuario, lista de IDs de negocios, y matrices de características.
2. Cálculo:
    - El modelo calcula puntuaciones de relevancia para cada negocio.
3. Salida:
    - Retorna los top_n negocios más relevantes.


## 🚀 Despliegue en Google Cloud Run

El despliegue está automatizado usando **GitHub Actions**. Al hacer un push en la rama `main`, se activa un workflow que:

1. Construye una imagen de Docker.
2. La sube al **Container Registry** de Google Cloud.
3. Despliega la imagen en **Cloud Run**.

### Configuración del Workflow
Los secretos necesarios en GitHub Actions:
- `GCP_PROJECT_ID`: ID del proyecto de Google Cloud.
- `GCP_SA_KEY`: Clave del servicio (Service Account Key) en formato JSON.

Para más detalles, consulta [`.github/workflows/deploy.yml`](./.github/workflows/deploy.yml).

<div align="center">
<img src="https://d3uyj2gj5wa63n.cloudfront.net/wp-content/uploads/2021/02/fastapi-logo.png" alt="Fastapi Logo" width="300">
</div>