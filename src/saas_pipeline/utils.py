import logging
from delta.tables import DeltaTable

logger = logging.getLogger(__name__)


def write_delta_overwrite(df, path, partition_by=None, merge_schema=False):
    """Realiza un OVERWRITE total de la tabla (Full Load)."""
    logger.info(f"Escribiendo OVERWRITE total en {path}")
    writer = df.write.format("delta").mode("overwrite")
    if merge_schema:
        writer = writer.option("mergeSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


def write_delta_merge(spark, df, path, merge_condition, partition_by=None):
    """
    Realiza un MERGE INTO (Upsert). Si la tabla no existe, la crea.
    Útil para Tablas de Hechos y Dimensiones (Idempotencia sin duplicados).
    """
    if DeltaTable.isDeltaTable(spark, path):
        logger.info(f"Aplicando MERGE INTO en {path} con condición: {merge_condition}")
        delta_table = DeltaTable.forPath(spark, path)
        (delta_table.alias("target")
         .merge(df.alias("source"), merge_condition)
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
    else:
        logger.info(f"Creando tabla por primera vez (OVERWRITE) en {path}")
        writer = df.write.format("delta").mode("overwrite")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(path)


def write_delta_replace_where(df, path, replace_condition, partition_by=None, merge_schema=False):
    """Realiza un OVERWRITE seguro particionado garantizando Idempotencia."""
    logger.info(f"Escribiendo REPLACE WHERE en {path} con condición: {replace_condition}")
    writer = df.write.format("delta").mode("overwrite").option("replaceWhere", replace_condition)
    if merge_schema:
        writer = writer.option("mergeSchema", "true")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


def write_delta_append(df, path, merge_schema=False):
    """Realiza un APPEND. Útil para tablas de solo inserción (ej. logs de eventos)."""
    logger.info(f"Agregando datos (APPEND) en {path}")
    writer = df.write.format("delta").mode("append")
    if merge_schema:
        writer = writer.option("mergeSchema", "true")
    writer.save(path)
