# Pipeline ETL de Yelp y Google en Airflow 🚀

## Descripción General 📝
Este proyecto implementa un pipeline ETL (Extracción, Transformación, Carga) utilizando Google Cloud Platform (GCP) y Apache Airflow. El objetivo es procesar y cargar datos de Yelp y Google en BigQuery de manera automatizada, asegurando que los datos estén limpios y listos para el análisis. Actualmente, el pipeline procesa los datos de check-in de Yelp, almacenándolos en una tabla temporal en BigQuery antes de realizar transformaciones adicionales.

## Tecnologías Utilizadas 💻
- **Google Cloud Platform (GCP)**:
  - BigQuery para el almacenamiento y procesamiento de datos.
  - Google Cloud Storage (GCS) para almacenar los archivos fuente.
  - Google Composer para administrar la arquitectura de Airflow.
- **Apache Airflow**: Orquestación del pipeline ETL.
- **Python**: Desarrollo de funciones auxiliares y transformación de datos.

## Estructura del Proyecto 📂
- **DAG**:
  - `DAG_yelp_etl.py`: Este archivo contiene el DAG principal que coordina las tareas de creación de tablas, extracción de datos y carga en BigQuery.
- **Módulos Auxiliares**:
  - `extract_data_yelp.py`: Contiene funciones para la extracción de datos desde Google Cloud Storage.
  - `transform_data_yelp.py`: Gestiona las transformaciones específicas para cada archivo de datos.
  - `load_data_yelp.py`: Incluye funciones para cargar los datos en BigQuery y gestionar las tablas.
- **Funciones de Utilidad**:
  - `bigquery_utils.py`: Se utilizó inicialmente para funciones generales, ahora organizadas en módulos separados.

## Estructura del DAG 🗂️

1. **Inicio**:
    - `inicio`: Un `DummyOperator` que marca el comienzo del DAG para facilitar la visualización en Airflow.
2. **Creación de Tabla Temporal**:
    - `crear_tabla_temporal`: Tarea encargada de crear una tabla temporal en BigQuery para almacenar los datos de los archivos en Storage con el esquema adecuado.
3. **Extracción y Transformación de Datos**:
    - `cargar_archivo_en_tabla_temporal`: Esta tarea extrae el archivo desde Google Cloud Storage (GCS), aplica las transformaciones necesarias y carga los datos en la tabla temporal en BigQuery.
4. **Fin**:
    - `fin`: Un `DummyOperator` que marca el fin del DAG.

## Estructura de Módulos y Funciones 🔧
- **extract_data_yelp.py**:
  - `cargar_archivo_gcs_a_dataframe`: Extrae un archivo de GCS y lo convierte en un DataFrame, aplicando transformaciones si es necesario.
- **transform_data_yelp.py**:
  - `pre_transformar_checkin`: Procesa el campo `date` en `checkin.json`, separando fechas en filas individuales y asegurando el formato TIMESTAMP.
  - `aplicar_transformacion`: Aplica una transformación específica en función del nombre del archivo, usando un diccionario de transformaciones.
- **load_data_yelp.py**:
  - `crear_tabla_temporal`: Crea la tabla temporal en BigQuery con el esquema especificado.
  - `cargar_dataframe_a_bigquery`: Carga un DataFrame en una tabla de BigQuery.

## Esquema de la Tabla Temporal 📋
La tabla temporal `checkin_temp` contiene los siguientes campos:
- **business_id**: Identificador del negocio (STRING).
- **date**: Fecha y hora del check-in en formato TIMESTAMP.

## Funcionalidades y Detalles Técnicos ⚙️
- **Diccionario de Transformaciones**: En `transform_data_yelp.py`, el diccionario `transformaciones` asocia cada archivo con su respectiva función de transformación. Esto facilita la extensión del pipeline a otros archivos: solo se necesita crear una función de transformación y añadirla al diccionario.
- **Validaciones y Control de Errores**: El pipeline incluye validaciones, como verificar si el DataFrame está vacío antes de cargarlo en BigQuery. Los bloques `try-except` se eliminaron para mejorar la transparencia y permitir que los errores se manejen adecuadamente a través de los registros de Airflow.
- **Modularización del Código**: Las funciones de extracción, transformación y carga se han reorganizado en módulos independientes (`extract_data_yelp.py`, `transform_data_yelp.py` y `load_data_yelp.py`) para mejorar la claridad y la reutilización del código.

## Próximos Pasos 🔜
- **Implementación de Transformación en BigQuery**:
  - Realizar transformaciones adicionales en BigQuery sobre la tabla temporal (`checkin_temp`) antes de mover los datos a la tabla final.
  - Eliminar la tabla temporal una vez que los datos hayan sido transformados y cargados en la tabla final, para optimizar el uso de recursos.
- **Automatización para Otros Datasets**:
  - Extender el pipeline para procesar otros archivos de Yelp y Google, agregando sus transformaciones específicas al diccionario `transformaciones`.
- **Automatización del Pipeline**:
  - Configurar sensores o triggers en Airflow para que el pipeline se active automáticamente al detectar nuevos archivos en GCS.
- **Monitoreo y Alertas**:
  - Implementar alertas y notificaciones en caso de fallos en alguna de las tareas del DAG, utilizando los servicios de Airflow y GCP.

## Posibles Mejoras y Consideraciones Futuras 🌟
- **Optimización de Transformaciones en BigQuery**:
  - A medida que se agreguen más datos, puede ser necesario optimizar las consultas de transformación en BigQuery para reducir costos y tiempo de procesamiento.
- **Documentación y Comentarios en el Código**:
  - Es importante mantener la documentación del código y actualizar el markdown conforme avancemos en el proyecto. Esto facilitará la colaboración en equipo y la comprensión de los cambios realizados.
- **Manejo de Versiones**:
  - Considerar la posibilidad de versionar el código y los datasets para mantener un historial de cambios y facilitar la reversión en caso de problemas.

## Notas Finales 📝
Este markdown documenta el progreso actual del pipeline y proporciona una guía sobre la arquitectura, el flujo de trabajo y las consideraciones clave. A medida que avancemos en el proyecto, este documento se actualizará para reflejar nuevos desarrollos y decisiones de diseño.
