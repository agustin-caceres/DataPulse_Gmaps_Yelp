# Pipeline ETL de Yelp y Google en Airflow 🚀

## Descripción General 📝

Este proyecto implementa un pipeline ETL (Extracción, Transformación, Carga) utilizando Google Cloud Platform (GCP) y Apache Airflow. El objetivo es procesar y cargar datos de Yelp y Google en BigQuery de manera automatizada, asegurando que los datos estén limpios y listos para el análisis. Actualmente, el pipeline procesa los datos, almacenándolos en tablas temporales en BigQuery antes de realizar transformaciones adicionales y cargar los datos en tablas finales.

## Tecnologías Utilizadas 💻

- **Google Cloud Platform (GCP)**:
  - BigQuery para el almacenamiento y procesamiento de datos.
  - Google Cloud Storage (GCS) para almacenar los archivos fuente.
  - Google Composer para administrar la arquitectura de Airflow.
- **Apache Airflow**: Orquestación del pipeline ETL.
- **Python**: Desarrollo de funciones auxiliares y transformación de datos.

## Estructura del Proyecto 📂

- **DAGs**:
  - `DAG_yelp.py`: Contiene el DAG principal que coordina las tareas de creación de tablas, verificación de archivos procesados, extracción de datos y carga en BigQuery.
- **Módulos Auxiliares**:
  - **Extracción de Datos:**
    - `extract_data_yelp.py`: Funciones para la extracción de datos desde Google Cloud Storage.
  - **Transformación de Datos**
    - `transform_data_yelp.py`: Gestiona las transformaciones específicas para cada archivo de datos, con funciones adaptadas para `checkin.json` y `tip.json`.
  - **Carga de Datos**
    - `load_data_yelp.py`: Incluye funciones para cargar los datos en BigQuery y gestionar las tablas.

## Estructura de los DAGs: 🗂️

### **``ETL_Yelp DAG:``**
- **Inicio**:
  - `inicio`: Un `DummyOperator` que marca el comienzo del DAG para facilitar la visualización en Airflow.
- **Verificación de Archivos Procesados**:
  - `verificar_archivo_procesado`: Utiliza `BranchPythonOperator` para verificar si el archivo ya fue procesado consultando la tabla de control en BigQuery.
- **Creación de Tablas Temporales**:
  - `crear_tabla_temporal`: Crea tablas temporales en BigQuery para almacenar los datos con el esquema adecuado.
- **Extracción y Transformación de Datos**:
  - `cargar_archivo_en_tabla_temporal`: Extrae los archivos desde Google Cloud Storage (GCS), aplica transformaciones necesarias y carga los datos en las tablas temporales en BigQuery.
- **Transformación en BigQuery y Carga en Tablas Finales**:
  - `transformar_checkin` y `transformar_tip`: Transforman los datos en las tablas temporales y los cargan en las tablas finales `checkin_yelp` y `tip_yelp`.
- **Eliminación de Tablas Temporales**:
  - `eliminar_tabla_temporal`: Elimina las tablas temporales después de cargar los datos en las tablas finales para optimizar el uso de recursos.
- **Registro en la Tabla de Control**:
  - `registrar_archivo_procesado`: Registra el archivo como procesado en la tabla de control para evitar reprocesamiento.
- **Fin**:
  - `fin_checkin` y `fin_tip`: `DummyOperator` que marca el final de cada flujo para `checkin.json` y `tip.json`.

## Diagrama del DAGs 📊

### **``ETL_Yelp DAG:``**

Este diagrama muestra cómo el flujo del DAG se adapta para diferentes archivos (checkin y tip) y cómo se asegura la modularidad y flexibilidad en el procesamiento.

![Diagrama del DAG](../../assets/Images/dag_yelp.png)


## Estructura de Módulos y Funciones 🔧

- **extract\_data\_yelp.py**:
  - `cargar_archivo_gcs_a_dataframe`: Extrae un archivo de GCS y lo convierte en un DataFrame, aplicando transformaciones si es necesario.
- **transform\_data\_yelp.py**:
  - `pre_transformar_checkin`: Procesa el campo `date` en `checkin.json`, separando fechas en filas individuales y asegurando el formato `TIMESTAMP`.
  - `pre_transformar_tip`: Procesa `tip.json`, eliminando valores nulos en `text` y `date`, y asegura que `date` esté en formato `DATE`.
  - `aplicar_transformacion`: Aplica una transformación específica en función del nombre del archivo.
- **load\_data\_yelp.py**:
  - `crear_tabla_temporal`: Crea la tabla temporal en BigQuery con el esquema especificado.
  - `cargar_dataframe_a_bigquery`: Carga un DataFrame en una tabla de BigQuery.
  - `eliminar_tabla_temporal`: Elimina una tabla temporal en BigQuery una vez que los datos han sido transferidos a la tabla final.
  - `archivo_procesado` y `registrar_archivo_procesado`: Gestionan el registro y verificación de archivos en la tabla de control para evitar reprocesamiento.

## Esquemas de las Tablas en BigQuery 📋

- **checkin\_temp**:
  - `business_id`: Identificador del negocio (STRING).
  - `date`: Fecha y hora del check-in en formato `TIMESTAMP`.
- **tip\_temp**:
  - `text`: Texto del consejo (STRING).
  - `date`: Fecha del consejo (DATE).
  - `compliment_count`: Cantidad de cumplidos (INTEGER).
  - `business_id`: Identificador del negocio (STRING).
  - `user_id`: Identificador del usuario (STRING).

## Funcionalidades y Detalles Técnicos ⚙️

- **Diccionario de Transformaciones**: En `transform_data_yelp.py`, el diccionario `transformaciones` asocia cada archivo con su respectiva función de transformación, facilitando la extensión del pipeline a otros archivos. Solo se necesita crear una función de transformación y añadirla al diccionario.
- **Verificación de Archivos Procesados**: El pipeline verifica si cada archivo ha sido procesado anteriormente consultando una tabla de control en BigQuery. Esto evita procesamientos duplicados.
- **Modularización del Código**: Las funciones de extracción, transformación y carga están organizadas en módulos independientes para mejorar la claridad y la reutilización del código.
- **Estrategia para Versionado Futuro de Archivos**: Se plantea la posibilidad de optimizar el pipeline para manejar nuevas versiones de archivos que reemplacen a los anteriores, permitiendo su procesamiento sin conflicto en la tabla de control.

## Próximos Pasos 🔜

- **Optimización del Procesamiento de Nuevas Versiones de Archivos**:
  - Implementar una estrategia para detectar cambios en archivos existentes en el bucket de GCS, permitiendo la actualización de versiones de los datasets.
- **Automatización para Otros Datasets**:
  - Extender el pipeline para procesar otros archivos de Yelp y Google, agregando sus transformaciones específicas al diccionario `transformaciones`.
- **Monitoreo y Alertas**:
  - Implementar alertas y notificaciones en caso de fallos en alguna de las tareas del DAG, utilizando los servicios de Airflow y GCP.

## Posibles Mejoras y Consideraciones Futuras 🌟

- **Optimización de Transformaciones en BigQuery**:
  - A medida que se agreguen más datos, puede ser necesario optimizar las consultas de transformación en BigQuery para reducir costos y tiempo de procesamiento.
- **Documentación y Comentarios en el Código**:
  - Mantener la documentación actualizada conforme se avanza en el proyecto para facilitar la colaboración en equipo.
- **Versionado de Código y Datos**:
  - Considerar el uso de herramientas de control de versiones para el código y el manejo de versiones de datasets en GCS para mantener un historial claro y facilitar el rollback.

## Notas Finales 📝

Este markdown documenta el progreso actual del pipeline y proporciona una guía sobre la arquitectura, el flujo de trabajo y las consideraciones clave. Se actualizará para reflejar nuevos desarrollos y decisiones de diseño a medida que avancemos en el proyecto.

