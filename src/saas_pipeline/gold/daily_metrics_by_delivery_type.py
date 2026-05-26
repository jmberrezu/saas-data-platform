import logging
import pyspark.sql.functions as F
from src.saas_pipeline.config import load_config
from src.saas_pipeline.bronze.bronze import get_spark_session
from src.saas_pipeline.utils import write_delta_replace_where

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def process_gold_daily_metrics(tenant: str, env: str = "dev", start_date: str = None, end_date: str = None):
    """
    Construye la tabla Gold: daily_metrics_by_delivery_type.
    Granularidad: tenant_id, fecha_proceso, tipo_entrega.
    """
    conf = load_config(tenant=tenant, env=env)
    spark = get_spark_session()

    silver_deliveries_path = f"{conf.paths.silver}/{tenant.lower()}/fact_deliveries"
    gold_output_path = f"{conf.paths.gold}/{tenant.lower()}/daily_metrics_by_delivery_type"

    logger.info(f"Iniciando procesamiento Gold para el tenant: {tenant.upper()}")

    try:
        df_silver = spark.read.format("delta").load(silver_deliveries_path) \
                         .filter(F.col("_tenant_id") == tenant.lower())

        if start_date:
            df_silver = df_silver.filter(F.col("fecha_proceso").cast("string") >= start_date.replace("-", ""))
        if end_date:
            df_silver = df_silver.filter(F.col("fecha_proceso").cast("string") <= end_date.replace("-", ""))

    except Exception as e:
        logger.error(f"Error al leer fact_deliveries en Silver: {e}")
        return

    # --- AGREGACIONES DE NEGOCIO (Regla 6.4) ---
    df_gold = (
        df_silver
        .groupBy("_tenant_id", "fecha_proceso", "tipo_entrega")
        .agg(
            # Suma de cantidad ya normalizada a ST
            F.sum("cantidad_normalizada_st").alias("total_units"),
            # Revenue = cantidad_normalizada * precio_transaccion
            F.sum(F.col("cantidad_normalizada_st") * F.col("precio")).alias("total_revenue"),
            # Rutas y Transportes únicos
            F.countDistinct("ruta").alias("active_routes"),
            F.countDistinct("transporte").alias("active_transports")
        )
    )

    # --- ESCRITURA (Regla 5.5 - Recomputo completo por partición) ---
    unique_dates = [
        row["fecha_proceso"]
        for row in df_gold.select("fecha_proceso").distinct().collect()
    ]

    if not unique_dates:
        logger.warning(f"No hay datos procesables para Gold en el tenant {tenant}")
        return

    # Confiamos en el contrato de datos de Silver: No hay fechas nulas.
    dates_sql = ", ".join([f"'{d}'" for d in unique_dates])
    replace_condition = f"fecha_proceso IN ({dates_sql})"

    logger.info(f"Escribiendo métricas en la capa Gold con condición: {replace_condition}")

    write_delta_replace_where(
        df_gold, gold_output_path, replace_condition, partition_by=["fecha_proceso"]
    )

    logger.info("Capa Gold procesada exitosamente.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Procesamiento Capa Gold - Métricas Diarias")
    parser.add_argument("--tenant", type=str, required=True, help="Código del tenant (ej. ec)")
    parser.add_argument("--env", type=str, default="dev", help="Entorno de ejecución (dev, qa, main)")
    parser.add_argument("--start-date", type=str, help="Fecha de inicio (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="Fecha de fin (YYYY-MM-DD)")
    args = parser.parse_args()
    process_gold_daily_metrics(tenant=args.tenant, env=args.env, start_date=args.start_date, end_date=args.end_date)
