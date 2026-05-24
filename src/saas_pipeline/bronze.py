from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit, lower, col
from src.saas_pipeline.config import load_config
import uuid
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def get_spark_session():
    return SparkSession.builder \
        .appName("SAAS_Bronze_Layer") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()


def process_bronze(tenant: str, entity: str):
    # --- CONFIGURACIÓN Y RUTAS ---
    conf = load_config(tenant=tenant)
    spark = get_spark_session()

    if entity not in conf.schemas:
        raise ValueError(f"Error: La entidad '{entity}' no está definida en el archivo base.yaml")

    entity_conf = conf.schemas[entity]

    raw_path = f"{conf.paths.raw}/{entity_conf.file_name}"
    bronze_path = f"{conf.paths.bronze}/{entity}"

    logger.info(
        f"Iniciando procesamiento Bronze para el tenant: {tenant.upper()} | "
        f"Entidad: {entity.upper()}"
    )

    # --- INGESTA Y CONTRATO DE ESQUEMA ---
    df_raw = spark.read.csv(raw_path, header=True, inferSchema=True)

    columnas_esperadas = [c.lower() for c in df_raw.columns]
    columnas_requeridas = entity_conf.required_columns

    columnas_faltantes = [c for c in columnas_requeridas if c.lower() not in columnas_esperadas]

    if columnas_faltantes:
        error_msg = (
            f"Error crítico en {entity}: Faltan las columnas "
            f"obligatorias {columnas_faltantes}"
        )
        if conf.get("execution", {}).get("fail_fast", True):
            raise ValueError(error_msg)
        else:
            logger.error(f"{error_msg} -> Omitiendo archivo debido a fail_fast=False")
            return

    # --- ENRIQUECIMIENTO Y AISLAMIENTO DE TENANT ---
    batch_id = str(uuid.uuid4())
    columna_tenant_origen = entity_conf.get("tenant_column", None)

    if columna_tenant_origen:
        # Tablas transaccionales: se filtran y aíslan por el país/tenant especificado
        df_bronze = df_raw.filter(lower(col(columna_tenant_origen)) == tenant.lower())
        df_bronze = df_bronze.withColumn("_tenant_id", lower(col(columna_tenant_origen)))
    else:
        # Tablas de catálogo/dimensiones: son globales y aplican a todos los tenants
        df_bronze = df_raw
        df_bronze = df_bronze.withColumn("_tenant_id", lit("global"))

    # Metadatos de auditoría requeridos en capa Bronze
    df_bronze = (
        df_bronze.withColumn("_ingestion_timestamp", current_timestamp())
                 .withColumn("_source_file", input_file_name())
                 .withColumn("_batch_id", lit(batch_id))
    )

    # --- ESCRITURA EN DELTA E IDEMPOTENCIA ---
    columna_particion = entity_conf.get("partition_column", None)

    if columna_particion:
        # Estrategia replaceWhere: Sobrescribe únicamente las particiones presentes en este lote
        valores_unicos = [
            row[columna_particion]
            for row in df_bronze.select(columna_particion).distinct().collect()
        ]

        if not valores_unicos:
            logger.warning(f"No hay datos procesables para el tenant {tenant}")
            return

        valores_validos = [v for v in valores_unicos if v is not None]
        tiene_nulos = None in valores_unicos

        condiciones_particion = []
        if valores_validos:
            valores_sql = ", ".join([f"'{v}'" for v in valores_validos])
            condiciones_particion.append(f"{columna_particion} IN ({valores_sql})")

        if tiene_nulos:
            condiciones_particion.append(f"{columna_particion} IS NULL")

        filtro_particion = " OR ".join(condiciones_particion)

        condicion_reemplazo = f"_tenant_id = '{tenant}' AND ({filtro_particion})"

        logger.info(f"Escribiendo en Delta con condición (Idempotencia): {condicion_reemplazo}")

        (df_bronze.write
            .format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .option("replaceWhere", condicion_reemplazo)
            .partitionBy(columna_particion, "_tenant_id")
            .save(bronze_path))
    else:
        # Los catálogos globales se sobrescriben en su totalidad (Full Load)
        logger.info(f"Escribiendo catálogo global '{entity}' en modo overwrite total")
        (df_bronze.write
            .format("delta")
            .mode("overwrite")
            .option("mergeSchema", "true")
            .save(bronze_path))

    logger.info("Capa Bronze procesada exitosamente.")


if __name__ == "__main__":
    process_bronze(tenant="ec", entity="deliveries")
