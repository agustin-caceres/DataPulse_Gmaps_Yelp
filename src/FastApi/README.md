
# DataPulse Analytics - FastAPI Backend


Este backend es parte de un sistema diseñado para ofrecer recomendaciones personalizadas de negocios (restaurantes, hoteles, etc.) basándose en las preferencias de los usuarios y datos de ubicación. Utiliza modelos de machine learning y está diseñado para integrarse con una aplicación de Streamlit como frontend.

## 🛠️ Arquitectura del Sistema
El sistema tiene tres componentes principales:

1. **Backend (FastAPI):** Nuestro servidor que recibe solicitudes, aplica el modelo de ML y devuelve respuestas en JSON.
- Cómo funciona:
  - Define endpoints como /get_recomendations para recibir parámetros de entrada (estado, categoría, etc.).
  - Usa un modelo preentrenado para generar recomendaciones personalizadas.
  - Devuelve un JSON estructurado con las recomendaciones.

2. **Frontend (Streamlit)** Nuestra app que servira el modelo en una interfaz visual para que los usuarios interactúen con el sistema.
-  Cómo funciona:
    - Recoge los datos ingresados por los usuarios (estado, categoría, etc.).
    - Realiza solicitudes HTTP a la API del backend.
    - Muestra los resultados en forma de listas, gráficos o mapas.

3. **CI/CD y Despliegue**
- Pipeline de CI/CD:
  - Automatiza la construcción y despliegue del backend con GitHub Actions.
  - Cada cambio en la rama main genera automáticamente un nuevo despliegue en Google Cloud Run.
- Google Cloud Run:
  - Servicio que ejecuta el backend como un contenedor escalable en la nube.

## 🚀 Flujo de Datos
- **Entrada**: El usuario ingresa los siguientes datos en la app de Streamlit:
  - ``state:`` Estado donde quiere las recomendaciones (por ejemplo, "California").
  - ``user_id_str:`` Identificador del usuario.
  - ``category:`` Categoría del negocio (por ejemplo, "Restaurants").
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
        "business_id": "123",
        "name": "Italian Food",
        "city": "Los Angeles",
        "address": "123 street",
        "latitude": 34.052235,
        "longitude": -118.243683
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


## 📋 Endpoints Disponibles


### **`GET /`**
- Responde con un mensaje de bienvenida y un enlace a la documentación Swagger.

### **`GET /get_recomendations`**
- **Descripción**: Devuelve recomendaciones personalizadas basadas en las preferencias del usuario.
- **Parámetros del payload**:
  ```json
  {
    "state": "California",
    "user_id_str": "user123",
    "category": "Restaurants",
    "top_n": 5
  }
  ```
- **Respuesta**:
  ```json
  {
    "recomendations": [
      {
        "business_id": "123",
        "name": "Italian Food",
        "city": "Los Angeles",
        "address": "123 street",
        "latitude": 34.052235,
        "longitude": -118.243683
      }
    ]
  }
  ```


## 🧠 Modelo de Machine Learning
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


## 📦 Despliegue en Google Cloud Run

El despliegue está automatizado usando **GitHub Actions**. Al hacer un push en la rama `main`, se activa un workflow que:

1. Construye una imagen de Docker.
2. La sube al **Container Registry** de Google Cloud.
3. Despliega la imagen en **Cloud Run**.

### Configuración del Workflow
Los secretos necesarios en GitHub Actions:
- `GCP_PROJECT_ID`: ID del proyecto de Google Cloud.
- `GCP_SA_KEY`: Clave del servicio (Service Account Key) en formato JSON.

Para más detalles, consulta [`.github/workflows/deploy.yml`](./.github/workflows/deploy.yml).



## 📋 Próximos pasos

- **Integración con Streamlit**: Completar las pruebas para garantizar la comunicación correcta entre el frontend y el backend.
- **Optimización del rendimiento**: Mejorar la carga de recursos y la eficiencia de los filtros.
- **Monitoreo y logs:**: Configurar herramientas para monitorear el rendimiento y errores en producción.