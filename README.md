# Insights_IMDB

Proyecto de ingeniería de datos (ETL) para centralizar y transformar datos de IMDB. Este repositorio implementa un pipeline ETL que:
- Extrae y carga (EL) datos con Python (DLT).
- Transforma datos con dbt.
- Orquesta todo con Apache Airflow.

Objetivo: entregar un pipeline reproducible, versionado y observable para preparar un modelo de datos listo para análisis y visualización.
Se desarrolló un informe en Power BI con insights de negocio.

## Arquitectura (resumen)
1. Extracción y carga (EL)
   - Script Python (librería DLT) encargado de la ingesta desde la fuente (archivo csv) y la carga en la zona raw/bronze.
   - Diseñado para ejecutarse como pipeline.
2. Transformación
   - dbt para transformar y documentar los modelos (silver/gold).
   - Tests y documentación con dbt.
3. Orquestación
   - Airflow gestiona dependencias entre tareas: ejecutar DLT, esperar finalización, lanzar dbt, ejecutar tests, notificar.
4. Almacenamiento / Data Warehouse
   - Conexión configurable (p. ej. data lake, Delta Lake, BigQuery, Snowflake). En este caso se usó una base de datos PostgreSQL. Configurar target en dbt y conexiones en Airflow.

Diagrama simplificado:
raw -> (DLT ETL) -> bronze -> (dbt) -> silver -> (dbt) -> gold -> consumos

## Requisitos
- Python 3.8+ (recomendar crear un virtualenv)
- dbt (versión acorde al adapter elegido: dbt-core + adapter)
- Apache Airflow (2.x)
- Cliente / SDK del Data Warehouse que uses (p. ej. databricks-cli, google-cloud-bigquery, snowflake-connector), en este caso PostgreSQL.

## Estructura del repo
- src/
  - dlt/                # Scripts de extracción y carga (Python pipelines)
  - dbt/                # Proyecto dbt (models, seeds, snapshots, macros)
  - airflow/            # DAGs y operadores personalizados

## Testing
- dbt tests (singular/relationships) para integridad.
