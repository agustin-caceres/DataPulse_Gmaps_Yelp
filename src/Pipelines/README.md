# Pipeline ETL de Yelp y Google en Airflow

## Descripción General
Este proyecto implementa un pipeline ETL (Extracción, Transformación, Carga) utilizando Google Cloud Platform (GCP) y Apache Airflow. El objetivo es procesar y cargar los datos de Yelp y Google en BigQuery de manera automatizada, asegurando que los datos estén limpios y listos para el análisis. Actualmente, el pipeline está configurado para procesar los datos de check-in de Yelp y almacenarlos en una tabla temporal en BigQuery antes de realizar transformaciones adicionales.

## Tecnologías Utilizadas
- **Google Cloud Platform (GCP)**:
  - BigQuery para el almacenamiento y procesamiento de datos.
  - Google Cloud Storage (GCS) para almacenar los archivos fuente.
- **Apache Airflow**: Orquestación del pipeline ETL.
- **Python**: Desarrollo de funciones auxiliares y transformación de datos.

## Estructura del Proyecto
- **DAG**:
    - `DAG_yelp_etl.py:` Este archivo contiene el DAG principal que coordina las tareas de creación de tablas, extracción de datos y carga en BigQuery.
- **Módulos Auxiliares**:
  - `bigquery_utils.py`: Incluye funciones auxiliares para interactuar con BigQuery, como la creación de tablas, carga de DataFrames, y la eliminación de tablas temporales.
  - `extract_data_yelp.py`: Contiene funciones para la extracción y transformación específicas de los datasets.
- **Transformaciones**: 
    - Transformaciones específicas para distintos archivos (por ejemplo, checkin.json) se gestionan mediante un diccionario en bigquery_utils.py, lo que permite agregar reglas de transformación específicas para otros archivos de manera sencilla.

## Estructura del DAG

1. **Inicio**: 
    - `Inicio:` Un `DummyOperator` que marca el comienzo del DAG para facilitar la visualización en Airflow.
2. **Creación de Tabla Temporal**: 
    - `crear_tabla_temporal:` Tarea encargada de crear una tabla temporal en BigQuery para almacenar los datos de los archivos en Storage con el esquema adecuado. Esto permite realizar transformaciones adicionales en BigQuery antes de mover los datos a la tabla final.
3. **Carga de Datos**: 
    - `cargar_archivo_en_tabla_temporal:` Esta tarea extrae el archivo desde Google Cloud Storage (GCS), aplica las transformaciones necesarias y carga los datos en la tabla temporal en BigQuery para luego realizar las transformaciones y mover los datos a la tabla final.
4. **Fin**: 
    - `fin:` Un `DummyOperator` que marca el fin del DAG.

## Esquema de la Tabla Temporal

La tabla temporal `checkin_temp` contiene los siguientes campos:
- **business_id**: Identificador del negocio (STRING).
- **date**: Fecha y hora del check-in en formato `TIMESTAMP`.

## Funcionalidades y Detalles Técnicos
- **Diccionario de Transformaciones**:
  - En `bigquery_utils.py` se encuentra un diccionario `transformaciones` que permite asociar cada archivo con su respectiva función de transformación. Esto facilita la extensión del pipeline a otros archivos, ya que solo se necesita crear una función de transformación y añadirla al diccionario.
- **Función de Transformación `transformar_checkin`**:
  - Esta función procesa el campo `date` en `checkin.json`, separando las fechas en filas individuales y convirtiendo cada valor en formato TIMESTAMP. La función asegura que las fechas estén en el formato `YYYY-MM-DD HH:MM:SS` antes de la carga en BigQuery.
- **Validaciones y Control de Errores**:
  - El pipeline incluye validaciones como verificar si el DataFrame está vacío antes de cargarlo en BigQuery.
  - En caso de error durante la extracción o carga, se utilizan bloques `try-except` para manejar las excepciones y permitir que el pipeline continúe sin interrupciones graves.

## Próximos Pasos
- **Implementación de Transformación en BigQuery**:
  - Realizar transformaciones adicionales en BigQuery sobre la tabla temporal (`checkin_temp`) antes de mover los datos a la tabla final.
  - Eliminar la tabla temporal una vez que los datos hayan sido transformados y cargados en la tabla final, para optimizar el uso de recursos.
- **Automatización para Otros Datasets**:
  - Extender el pipeline para procesar otros archivos de Yelp y Google, agregando sus transformaciones específicas al diccionario `transformaciones`.
- **Automatización del Pipeline**:
  - Configurar sensores o triggers en Airflow para que el pipeline se active automáticamente al detectar nuevos archivos en GCS.
- **Monitoreo y Alertas**:
  - Implementar alertas y notificaciones en caso de fallos en alguna de las tareas del DAG, utilizando los servicios de Airflow y GCP.

## Posibles Mejoras y Consideraciones Futuras
- **Optimización de Transformaciones en BigQuery**:
  - A medida que se agreguen más datos, puede ser necesario optimizar las consultas de transformación en BigQuery para reducir costos y tiempo de procesamiento.
- **Documentación y Comentarios en el Código**:
  - Es importante mantener la documentación del código y actualizar el markdown conforme avancemos en el proyecto. Esto facilitará la colaboración en equipo y la comprensión de los cambios realizados.
- **Manejo de Versiones**:
  - Considerar la posibilidad de versionar el código y los datasets para mantener un historial de cambios y facilitar la reversión en caso de problemas.


## Notas Finales
Este markdown documenta el progreso actual del pipeline y proporciona una guía sobre la arquitectura, el flujo de trabajo y las consideraciones clave. A medida que avancemos en el proyecto, este documento se actualizará para reflejar nuevos desarrollos y decisiones de diseño.
