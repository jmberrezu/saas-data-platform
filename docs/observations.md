# Observaciones a la Arquitectura Propuesta

## 1. Decisiones arquitectónicas con las que no estoy de acuerdo

## 2. Ambigüedades en la arquitectura y su resolución

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
