# Observaciones a la Arquitectura Propuesta

## 1. Decisiones arquitectónicas con las que no estoy de acuerdo

**1. Descartes silenciosos vs. Cuarentena Integral:**
* **Decisión provista:** La sección 5.6 instruye que los registros con `tipo_entrega` fuera de las 4 válidas deben descartarse (contabilizarse en métricas, pero no persistirse).
* **Desacuerdo:** En arquitecturas de gobierno de datos estricto, la pérdida física de datos imposibilita el análisis de causa raíz (*Root Cause Analysis*). Si un proveedor cambia su sistema y empieza a enviar cientos de códigos `Z99`, un analista no sabrá a qué fechas, rutas o transportes correspondían esos descartes porque fueron borrados de la plataforma.
* **Propuesta alternativa y Trade-offs:** Mi propuesta es enviar **todos** los datos anómalos a la tabla de Cuarentena, pero inyectando una columna de metadatos `acción_tomada` (ej. `quarantine` vs `discarded`). El trade-off principal es un ligero aumento en los costos de almacenamiento (Storage) para la tabla paralela, a cambio de maximizar la observabilidad, la trazabilidad y la resolución colaborativa de problemas con el negocio.

**2. Implementación nativa de SCD Type 2 en la Capa Silver:**
* **Decisión provista:** El catálogo de materiales se procesa y une en Silver como SCD Type 2 usando un `JOIN` temporal en cada ejecución.
* **Desacuerdo:** Ejecutar un *Left Join* con evaluación de rangos temporales (`BETWEEN valid_from AND valid_to`) en cada micro-batch o procesamiento diario en la tabla transaccional (Hechos) es computacionalmente muy costoso (*Compute-Heavy*) en Spark.
* **Propuesta alternativa y Trade-offs:** Implementar la tabla de materiales como una "Dimensión de Instantánea" (*Snapshot Dimension Table*) o pre-materializar las versiones en un *Feature Store*. El trade-off es sacrificar un poco la normalización del almacenamiento para ahorrar costos masivos de cómputo en el *cluster* de Databricks durante los cruces diarios.

## 2. Ambigüedades en la arquitectura y su resolución

**1. Nomenclatura Lógica vs. Física en Cuarentena:**
* **Ambigüedad:** La sección 5.2 indica la ruta física de cuarentena como `data/<layer>_quarantine/<tenant>/<table>/`, mientras que la sección 5.6 instruye escribir en la tabla paralela `<layer>_quarantine_<tenant>.<table>`.
* **Resolución:** Se interpretó la sección 5.6 como la convención de nomenclatura lógica para Unity Catalog / Hive Metastore (donde `silver_quarantine_ec` es el *Schema* y `fact_deliveries` es la *Tabla*). Por lo tanto, físicamente en el disco local se implementó de forma estricta la estructura jerárquica de la sección 5.2 (`data/silver_quarantine/ec/fact_deliveries`), preparándola para un mapeo directo (`LOCATION`) a tablas externas en el entorno Cloud.

## 3. Mejoras tecnológicas para próximas iteraciones (Horizonte 2-3)

**1. Evolución a Streaming (Databricks Auto Loader):**
* Migrar la ingesta *batch* de la capa Bronze hacia un flujo continuo estructurado utilizando **Auto Loader** (`cloudFiles`). Esto eliminará la necesidad de orquestadores externos programados por lotes y permitirá a la plataforma reaccionar de forma incremental a la llegada de cada nuevo archivo CSV de los proveedores en tiempo real.

**2. Calidad de Datos Declarativa (Delta Live Tables - DLT):**
* En lugar de mantener un motor de validación Custom con PySpark puro (*Dataframes y withColumn*), el pipeline debería refactorizarse hacia Databricks DLT utilizando `Expectations` (ej. `@dlt.expect_all_or_drop`). DLT abstrae el manejo de cuarentenas, genera tableros de observabilidad de calidad de datos automáticos y gestiona los reintentos de forma nativa.

**3. Gobierno Universal con Unity Catalog:**
* Sustituir el aislamiento físico simulado de carpetas por una integración total con **Unity Catalog**. Implementar esquemas manejados (`saas_prod.silver_gt.fact_deliveries`), habilitando políticas de acceso por filas (Row-Level Security), enmascaramiento dinámico de columnas PII y un linaje automático de extremo a extremo visible desde la UI de Databricks.

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

**4. Aislamiento Físico Estricto por Tenant (Multi-tenant):**
* **Decisión:** Las capas Bronze, Silver y Gold se escriben dinámicamente en rutas que incluyen el tenant explícitamente (`data/<layer>/<tenant>/<table>/`) y se eliminó el particionamiento por `_tenant_id`.
* **Por qué:** Cumple estrictamente con el requerimiento de aislamiento físico de la sección 5.2. Al separar los datos en carpetas raíz por tenant, se delega el aislamiento a la estructura de directorios (preparándolo para el esquema de Unity Catalog) en lugar de depender únicamente del particionamiento interno de Delta Lake. Esto resuelve de forma elegante la ambigüedad interpretativa con la sección 5.4.

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
