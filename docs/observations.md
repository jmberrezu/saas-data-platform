# Observaciones a la Arquitectura Propuesta

## 1. Decisiones arquitectónicas con las que no estoy de acuerdo

## 2. Ambigüedades en la arquitectura y su resolución

**1. Nomenclatura Lógica vs. Física en Cuarentena:**
* **Ambigüedad:** La sección 5.2 indica la ruta física de cuarentena como `data/<layer>_quarantine/<tenant>/<table>/`, mientras que la sección 5.6 instruye escribir en la tabla paralela `<layer>_quarantine_<tenant>.<table>`.
* **Resolución:** Se interpretó la sección 5.6 como la convención de nomenclatura lógica para Unity Catalog / Hive Metastore (donde `silver_quarantine_ec` es el *Schema* y `fact_deliveries` es la *Tabla*). Por lo tanto, físicamente en el disco local se implementó de forma estricta la estructura jerárquica de la sección 5.2 (`data/silver_quarantine/ec/fact_deliveries`), preparándola para un mapeo directo (`LOCATION`) a tablas externas en el entorno Cloud.

## 3. Mejoras tecnológicas para próximas iteraciones (Horizonte 2-3)

## 4. Diseño y Decisiones Arquitectónicas Implementadas (Capa Bronze)

**1. Arquitectura Guiada por Metadatos (Metadata-Driven):**
* **Decisión:** El script de Python es 100% agnóstico a la entidad (sin *hardcoding* de conceptos como 'entregas' o 'clientes').
* **Por qué:** Permite escalar la plataforma a decenas de orígenes nuevos simplemente agregando bloques al archivo de configuración `base.yaml`, sin necesidad de crear nuevos scripts ni modificar el código PySpark.

**2. Contrato de Esquema vs. Calidad de Datos:**
* **Decisión:** En la capa Bronze solo se valida la existencia de columnas estructurales obligatorias (*Schema Enforcement*). No se aplican reglas de calidad o limpieza de datos (ej. detectar fechas inválidas o precios negativos).
* **Por qué:** Respetando los principios de la Arquitectura Medallón, Bronze debe ser el espejo inmutable de la verdad cruda. La basura se ingiere tal cual; la limpieza, el filtrado y el desvío a cuarentena son responsabilidades exclusivas de la capa Silver.

**3. Idempotencia Dinámica (`replaceWhere`):**
* **Decisión:** Se genera una cláusula SQL dinámica para la opción `replaceWhere` de Delta Lake, basada en las particiones presentes en el lote de datos y aislando la operación por `_tenant_id`.
* **Por qué:** Garantiza que el *pipeline* pueda ejecutarse infinitas veces sin duplicar datos, protegiendo las particiones históricas y evitando sobreescribir datos de otros *tenants*. Se incluyó manejo explícito de particiones nulas (`IS NULL`) para evitar errores en el motor de Delta.

**4. Soporte Híbrido (Hechos vs. Dimensiones):**
* **Decisión:** La lógica lee del YAML para determinar si la entidad requiere `tenant_column` y `partition_column`.
* **Por qué:** Permite procesar con el mismo script tanto **Tablas Transaccionales** (ej. entregas, particionadas temporalmente y aisladas por inquilino) como **Catálogos Globales** (ej. materiales, cargas completas o *Full Load* compartidas transversalmente para todos los países).

**5. Trazabilidad y Linaje (Audit Columns):**
* **Decisión:** Se inyectan columnas técnicas de auditoría a nivel de registro (`_ingestion_timestamp`, `_source_file`, `_batch_id`, `_tenant_id`).
* **Por qué:** Habilita la auditoría estricta y el linaje de datos. Permite rastrear el origen de cualquier registro anómalo encontrado en las capas Silver/Gold hasta el archivo físico exacto del Data Lake desde el cual provino.

## 5. Diseño y Decisiones Arquitectónicas Implementadas (Capa Silver)

**1. Monitoreo Activo de Calidad (Quality Logs):**
* **Decisión:** Las validaciones evalúan su severidad (`critical`, `warning`) y se centralizan los conteos en la tabla `quality_logs` como eventos.
* **Por qué:** Mantiene un registro histórico del estado de salud de cada lote de datos. Permite construir tableros de observabilidad de datos (Data Observability) muy valorados en producción.

**2. Disyuntor (Circuit Breaker) del Pipeline:**
* **Decisión:** Si se evalúan reglas críticas que fallan y el parámetro `quality.fail_on_critical` es verdadero, el pipeline escribe los registros malos a cuarentena y luego aborta la ejecución mediante una excepción.
* **Por qué:** Previene que datos fundamentalmente corruptos gatillen tareas posteriores (Gold), aislando el daño mientras deja la cuarentena disponible para investigación y reprocesamiento inmediato.

**3. Estándar de Código (Alias `F` para PySpark):**
* **Decisión:** Todas las funciones de PySpark se importan usando el alias estándar de la industria (`import pyspark.sql.functions as F`).
* **Por qué:** Evita colisiones de nombres (*Namespace Collisions*) con funciones nativas de Python como `sum()`, `max()`, o `min()`, dejando el código limpio, explícito y previniendo errores de ejecución difíciles de rastrear.

**4. Aislamiento Físico de la Cuarentena (Multi-tenant):**
* **Decisión:** La cuarentena se escribe dinámicamente en la ruta `<layer>_quarantine_<tenant>/<table>` (ej. `silver_quarantine_ec/deliveries`) en lugar de una carpeta global.
* **Por qué:** Cumple el requerimiento de aislamiento por *tenant* y capa (Regla 5.6). Al usar prefijos físicos simulando bases de datos, permitimos configurar políticas de retención y accesos de seguridad a nivel de país sin mezclar datos corruptos de distintos inquilinos.

**5. Capa de Abstracción de Datos (I/O Utilities):**
* **Decisión:** Toda la lógica de escritura física en Delta Lake (`MERGE`, `replaceWhere`, `append`) se extrajo a un módulo genérico de utilidades (`utils.py`).
* **Por qué:** Respeta el principio DRY (*Don't Repeat Yourself*) y el patrón *Data Access Layer*. Mantiene los scripts del pipeline puros, enfocados únicamente en transformaciones y reglas de negocio, y facilita futuras migraciones del motor de almacenamiento.

## 6. Diseño y Decisiones Arquitectónicas Implementadas (Orquestación y DAGs)

**1. Modularización Orientada a Tareas (Task-Oriented Design):**
* **Decisión:** Se separó el procesamiento de la capa Silver en scripts independientes (`process_materials.py` y `process_deliveries.py`) en lugar de mantenerlos en un solo monolito.
* **Por qué:** Imita la estructura de un Grafo Acíclico Dirigido (DAG). Permite que herramientas como Airflow o Databricks Workflows orquesten, paralelizen y reintenten tareas individuales en caso de fallo sin tener que reprocesar todo un bloque lógico.

**2. Validación Fuerte de Dependencias (Fail-Fast):**
* **Decisión:** El script de entregas de Silver verifica explícitamente (`DeltaTable.isDeltaTable`) que la tabla origen `dim_materials` exista antes de intentar ejecutar el *Join* temporal.
* **Por qué:** En entornos distribuidos y orquestados, un error de dependencia es común. Abortar el proceso de inmediato con un mensaje descriptivo evita desperdiciar horas de cómputo en un *cluster* de Spark intentando procesar datos que irremediablemente fallarían más adelante.
