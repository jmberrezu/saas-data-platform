# Plataforma de Datos Multi-Tenant (Proyecto SAAS)

Implementación de una plataforma de datos analítica empresarial siguiendo la Arquitectura Medallón (Bronze, Silver, Gold) sobre PySpark y Delta Lake. Este repositorio resuelve la prueba técnica para Senior Data Engineer.

## Stack Tecnologico y Versiones

* **Python:** 3.11.x
* **PySpark:** 3.5.0
* **Delta Lake:** 3.1.0 (delta-spark)
* **Librerias Adicionales:** OmegaConf, Pytest, Flake8
* **CI:** GitHub Actions

## Estructura del Repositorio

```text
saas-data-platform/
├── .github/                 # Configuracion de GitHub Actions para CI/CD
├── config/                  # Archivos YAML de configuracion gestionados con OmegaConf
│   ├── env/                 # Configuraciones especificas por ambiente (dev, qa, main)
│   ├── tenants/             # Configuraciones especificas por unidad de negocio (ej. sv)
│   └── base.yaml            # Configuracion principal y rutas por defecto
├── data/raw/                # Carpeta requerida (crear localmente) para los CSVs crudos
├── docs/                    # Documentacion tecnica del proyecto e IaC
│   ├── infra.md             # Snippet de Terraform para el provisionamiento
│   └── observations.md      # Analisis critico de la arquitectura y propuestas de mejora
├── mentoring/               # Archivos para el ejercicio de Code Review
├── src/saas_pipeline/       # Modulo principal del pipeline de datos
│   ├── bronze/
│   │   └── bronze.py        # Capa de ingesta: de RAW a formato Delta (Idempotente)
│   ├── silver/
│   │   ├── dim_materials.py # Dimension con historial de cambios (SCD Tipo 2)
│   │   └── fact_deliveries.py # Tabla de hechos transaccionales y calidad de datos
│   ├── gold/
│   │   └── daily_metrics_by_delivery_type.py # Tablas agregadas para consumo de negocio
│   ├── config.py            # Logica de carga y parseo de parametros YAML
│   ├── orchestrator.py      # Controlador auxiliar para orquestar el DAG localmente
│   └── utils.py             # Capa de abstraccion de datos (I/O, Idempotencia)
├── tests/                   # Suite de pruebas automatizadas (Pytest)
│   ├── test_quality.py      # Validaciones de integridad y calidad de datos
│   └── test_silver_transforms.py # Pruebas unitarias de las transformaciones
├── .flake8                  # Reglas de estilo para el linter (estándar PEP8)
└── requirements.txt         # Dependencias estrictas del proyecto
```

## Instrucciones Reproducibles (Setup Local)

He diseñado este proyecto para que se ejecute localmente simulando las rutas físicas de un Data Lake mediante directorios.

### 1. Entorno Virtual e Instalación (Windows)
Para garantizar compatibilidad con las versiones especificadas, te recomiendo crear el entorno utilizando Python 3.11:

```powershell
# 1. Crear el entorno virtual con Python 3.11
py -3.11 -m venv venv

# 2. Activar el entorno (desde PowerShell)
.\venv\Scripts\activate

# 3. Actualizar pip e instalar dependencias
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### 2. Preparar los Datos Crudos (RAW)
Por mejores prácticas de versionado, he ignorado la carpeta `data/` en Git. Para que el pipeline funcione, es necesario que se construya la estructura de entrada para los datos crudos:
1. Crea la ruta `data/raw/` en la raíz del proyecto.
2. Pega dentro los archivos `global_mobility_data_entrega_productos.csv` y `materials_catalog.csv` provistos en el reto técnico.

### 3. Ejecutar Linter y Pruebas Unitarias
He configurado validaciones de calidad estática del código (PEP8) y pruebas automatizadas. Puedes ejecutarlas localmente así:
```powershell
flake8 src/ tests/ --ignore=E501
python -m pytest tests/ -v
```

### 4. Ejecución del Pipeline

He diseñado el pipeline para que soporte dos modalidades de ejecución simulando un entorno productivo.

#### A) Ejecución Modular (Paso a Paso)
En un entorno productivo como Databricks Workflows, cada script se ejecuta como una tarea independiente. Puedes procesar las capas aisladamente pasando los parámetros requeridos:

```powershell
# 1. Capa Bronze (Catálogos y Transacciones)
python -m src.saas_pipeline.bronze.bronze --tenant global --entity materials
python -m src.saas_pipeline.bronze.bronze --tenant ec --entity deliveries

# 2. Capa Silver (SCD2 y Hechos con Calidad)
python -m src.saas_pipeline.silver.dim_materials
python -m src.saas_pipeline.silver.fact_deliveries --tenant ec

# 3. Capa Gold (Métricas Agregadas)
python -m src.saas_pipeline.gold.daily_metrics_by_delivery_type --tenant ec
```

#### B) Ejecución vía Orquestador (Recomendado para revisión)
Para facilitar tu evaluación técnica, desarrollé un orquestador auxiliar que simula un DAG de ejecución. 
Para ejecutar el pipeline de extremo a extremo sobre todos los inquilinos:
```powershell
python -m src.saas_pipeline.orchestrator --tenant all
```

## Onboarding de un Nuevo Tenant

La arquitectura es 100% *Metadata-driven*. Para agregar un nuevo país (ej. `co` para Colombia):
1. **Infraestructura (IaC):** Provisionar mediante herramientas de Infraestructura como Código la estrategia definida en `docs/infra.md` (Contenedores físicos y el Schema lógico en Unity Catalog).
2. **Configuración:** Agregar `"co"` a la lista de tenants activos en `config/base.yaml`.
3. **Reglas Específicas (Opcional):** Si el tenant requiere reglas particulares, crear un archivo `config/tenants/co.yaml` que sobrescribirá las reglas base mediante OmegaConf.
4. **Ejecución:** El motor reconocerá y orquestará automáticamente al tenant en la siguiente ejecución del `orchestrator.py`, sin modificar una sola línea de código Python.

## Qué dejé fuera y por qué (Decisiones de Alcance / MVP)

1. **Unity Catalog / Hive Metastore:** 
   * **Por qué:** Al ser un MVP local de PySpark sin conexión a un Databricks Workspace real, el aislamiento lógico de catálogos no es factible. 
   * **Resolución:** Se simuló estrictamente el aislamiento físico usando prefijos y particionamiento en el sistema de carpetas (ej. guardando la cuarentena particionada en `data/silver_quarantine/ec/`), preparándolo para un mapeo directo (`LOCATION`) hacia un sistema de catálogos en el futuro.
2. **Streaming Ingestion (Auto Loader):** 
   * **Por qué:** La prueba proporcionó archivos CSV estáticos limitados. Implementar *Structured Streaming* habría añadido complejidad sin posibilidad de demostrar un flujo de *micro-batch* realista.
   * **Resolución:** La ingesta Bronze se construyó en modo *Batch* utilizando cargas estáticas protegidas con un patrón `replaceWhere` dinámico por partición para garantizar la idempotencia, simulando el comportamiento confiable que se espera de un pipeline de *streaming* robusto.
3. **Implementación de Terraform (HCL):** 
   * **Por qué:** Aunque comprendo los conceptos de aislamiento y provisionamiento automático, no poseo experiencia profunda escribiendo la sintaxis HCL.
   * **Resolución:** Preferí ser transparente en mis conocimientos y enfocarme en documentar la estrategia de manera clara en `docs/infra.md`, dejando la implementación de código fuera del alcance actual.