import logging
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# Se puede usar un logger para seguimiento y debugging
logger = logging.getLogger(__name__)


def process_deliveries(spark: SparkSession, file_path: str, country: str, output_path: str) -> None:
    """
    Procesa las entregas de rutina aplicando transformaciones vectorizadas en PySpark.
    """
    try:
        # 1. Lectura distribuida en Spark
        df = spark.read.csv(file_path, header=True, inferSchema=True)

        # 2. Filtrado y uso de funciones nativas de Spark para transformaciones
        df_transformed = (
            df.filter(F.col("pais") == country)
              .filter(F.col("tipo_entrega").isin("ZPRE", "ZVE1"))
              .withColumn("cantidad_st",
                          F.when(F.col("unidad") == "CS", F.col("cantidad") * 20)
                           .otherwise(F.col("cantidad")))
              .withColumn("total", F.col("cantidad_st") * F.col("precio"))
              .select(
                  F.col("pais"),
                  F.col("fecha_proceso").alias("fecha"),
                  F.col("material"),
                  F.col("cantidad_st"),
                  F.col("total")
              )
        )

        # 3. Escritura idempotente particionada usando formato Delta
        (df_transformed.write
            .format("delta")
            .mode("overwrite")
            .option("replaceWhere", f"pais = '{country}'")
            .partitionBy("pais", "fecha")
            .save(output_path))

        logger.info(f"Procesamiento exitoso para {country}. Registros guardados: {df_transformed.count()}")

    except Exception as e:
        logger.error(f"Error procesando {file_path}: {e}")
        raise
