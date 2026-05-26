import logging
import pyspark.sql.functions as F
from src.saas_pipeline.config import load_config
from src.saas_pipeline.bronze.bronze import get_spark_session
from src.saas_pipeline.utils import write_delta_merge

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def process_silver_materials():
    """
    Carga el catálogo dim_materials respetando el formato SCD2 del origen,
    asegurando tipos de datos y garantizando idempotencia mediante MERGE.
    """
    conf = load_config(tenant="base")
    spark = get_spark_session()

    bronze_path = f"{conf.paths.bronze}/materials"
    silver_path = f"{conf.paths.silver}/dim_materials"

    logger.info("Iniciando procesamiento de dim_materials (SCD Type 2)...")

    try:
        df_bronze = spark.read.format("delta").load(bronze_path)
    except Exception as e:
        logger.error(f"Error al leer catálogo Bronze: {e}")
        return

    df_silver = (
        df_bronze
        .withColumn("precio_base", F.col("precio_base").cast("double"))
        .withColumn("valid_from", F.to_date(F.col("valid_from").cast("string"), "yyyy-MM-dd"))
        .withColumn("valid_to", F.to_date(F.col("valid_to").cast("string"), "yyyy-MM-dd"))
        .withColumn("is_current", F.col("is_current").cast("boolean"))
    )

    logger.info("Escribiendo dim_materials de forma idempotente (MERGE)...")
    merge_cond = "target.material = source.material AND target.valid_from = source.valid_from"
    write_delta_merge(spark, df_silver, silver_path, merge_cond)

    logger.info("dim_materials procesada exitosamente.")


if __name__ == "__main__":
    process_silver_materials()
