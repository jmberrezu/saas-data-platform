from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from src.saas_pipeline.config import load_config
from src.saas_pipeline.utils import write_delta_replace_where, write_delta_overwrite
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

    expected_columns = [c.lower() for c in df_raw.columns]
    required_columns = entity_conf.required_columns

    missing_columns = [c for c in required_columns if c.lower() not in expected_columns]

    if missing_columns:
        error_msg = (
            f"Error crítico en {entity}: Faltan las columnas "
            f"obligatorias {missing_columns}"
        )
        if conf.get("execution", {}).get("fail_fast", True):
            raise ValueError(error_msg)
        else:
            logger.error(f"{error_msg} -> Omitiendo archivo debido a fail_fast=False")
            return

    # --- ENRIQUECIMIENTO Y AISLAMIENTO DE TENANT ---
    batch_id = str(uuid.uuid4())
    source_tenant_column = entity_conf.get("tenant_column", None)

    if source_tenant_column:
        # Tablas transaccionales: se filtran y aíslan por el país/tenant especificado
        df_bronze = df_raw.filter(F.lower(F.col(source_tenant_column)) == tenant.lower())
        df_bronze = df_bronze.withColumn("_tenant_id", F.lower(F.col(source_tenant_column)))
    else:
        # Tablas de catálogo/dimensiones: son globales y aplican a todos los tenants
        df_bronze = df_raw
        df_bronze = df_bronze.withColumn("_tenant_id", F.lit("global"))

    # Metadatos de auditoría requeridos en capa Bronze
    df_bronze = (
        df_bronze.withColumn("_ingestion_timestamp", F.current_timestamp())
                 .withColumn("_source_file", F.input_file_name())
                 .withColumn("_batch_id", F.lit(batch_id))
    )

    # --- ESCRITURA EN DELTA E IDEMPOTENCIA ---
    partition_column = entity_conf.get("partition_column", None)

    if partition_column:
        # Estrategia replaceWhere: Sobrescribe únicamente las particiones presentes en este lote
        unique_values = [
            row[partition_column]
            for row in df_bronze.select(partition_column).distinct().collect()
        ]

        if not unique_values:
            logger.warning(f"No hay datos procesables para el tenant {tenant}")
            return

        valid_values = [v for v in unique_values if v is not None]
        has_nulls = None in unique_values

        partition_conditions = []
        if valid_values:
            values_sql = ", ".join([f"'{v}'" for v in valid_values])
            partition_conditions.append(f"{partition_column} IN ({values_sql})")

        if has_nulls:
            partition_conditions.append(f"{partition_column} IS NULL")

        partition_filter = " OR ".join(partition_conditions)

        replace_condition = f"_tenant_id = '{tenant}' AND ({partition_filter})"

        logger.info(f"Escribiendo en Delta con condición (Idempotencia): {replace_condition}")

        write_delta_replace_where(
            df_bronze,
            bronze_path,
            replace_condition,
            partition_by=[partition_column, "_tenant_id"],
            merge_schema=True
        )
    else:
        # Los catálogos globales se sobrescriben en su totalidad (Full Load)
        write_delta_overwrite(df_bronze, bronze_path, merge_schema=True)

    logger.info("Capa Bronze procesada exitosamente.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Procesamiento Capa Bronze")
    parser.add_argument("--tenant", type=str, required=True, help="Código del tenant (ej. ec)")
    parser.add_argument("--entity", type=str, required=True, help="Entidad a procesar (ej. deliveries)")
    args = parser.parse_args()
    process_bronze(tenant=args.tenant, entity=args.entity)
