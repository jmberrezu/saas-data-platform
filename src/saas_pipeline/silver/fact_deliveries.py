import logging
import uuid
from pyspark.sql import Row
import pyspark.sql.functions as F
from delta.tables import DeltaTable
from src.saas_pipeline.config import load_config
from src.saas_pipeline.bronze.bronze import get_spark_session
from src.saas_pipeline.utils import (
    write_delta_merge,
    write_delta_replace_where,
    write_delta_append
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# --- FUNCIONES PURAS DE TRANSFORMACIÓN ---
def apply_deliveries_transformations(df_bronze):
    return (
        df_bronze
        .withColumn(
            "cantidad_normalizada_st",
            F.when(F.col("unidad") == "CS", F.col("cantidad") * 20)
            .otherwise(F.col("cantidad"))
        )
        .withColumn("unidad_estandar", F.lit("ST"))
        .withColumn(
            "fecha_proceso_dt",
            F.to_date(F.col("fecha_proceso").cast("string"), "yyyyMMdd")
        )
        .withColumn("is_routine_delivery", F.col("tipo_entrega").isin("ZPRE", "ZVE1"))
        .withColumn("is_bonus_delivery", F.col("tipo_entrega").isin("Z04", "Z05"))
    )


def apply_scd2_join(df_transformed, df_materials):
    temporal_join_condition = (
        (df_transformed.material == df_materials.material) & (df_transformed.fecha_proceso_dt.between(
            df_materials.valid_from, df_materials.valid_to
        ))
    )
    return df_transformed.join(df_materials, temporal_join_condition, how="left").select(
        df_transformed["*"],
        df_materials["precio_base"].alias("precio_catalogo_unitario")
    )


def evaluate_quality_rules(df, quality_rules):
    df_evaluated = df
    is_apt_for_silver_cond = F.lit(True)

    for rule_name, rule_config in quality_rules.items():
        if hasattr(rule_config, 'expr'):
            sql_expression = rule_config.expr  # Para obtener del yaml
        else:
            sql_expression = rule_config['expr']  # Para obtener de pruebas unitarias
        df_evaluated = df_evaluated.withColumn(f"_rule_{rule_name}", F.expr(sql_expression))
        is_apt_for_silver_cond = is_apt_for_silver_cond & F.col(f"_rule_{rule_name}")  # A nivel de registro, por todas las reglas

    return df_evaluated, is_apt_for_silver_cond


def record_quality_logs(spark, df_evaluated, quality_rules, _run_id, tenant, log_path):
    agg_exprs = [
        F.sum(F.when(~F.col(f"_rule_{rule_name}"), 1).otherwise(0))
        .alias(f"fails_{rule_name}")
        for rule_name in quality_rules.keys()
    ]
    agg_exprs.append(F.count(F.lit(1)).alias("total_records"))
    failure_counts = df_evaluated.agg(*agg_exprs).collect()[0]
    total_records_val = failure_counts["total_records"]
    has_critical_failures = False

    if total_records_val > 0:
        batch_id_val = df_evaluated.select("_batch_id").first()[0]
        quality_log_entries = []

        for rule_name, rule_config in quality_rules.items():
            if hasattr(rule_config, 'severity'):
                severity = rule_config.severity  # Para obtener del yaml
            else:
                severity = rule_config.get('severity', 'info')  # Para obtener de pruebas unitarias

            failures = failure_counts[f"fails_{rule_name}"]

            quality_log_entries.append(Row(
                _run_id=_run_id, _batch_id=str(batch_id_val), tenant_id=tenant.lower(),
                layer="silver", table_name="fact_deliveries", check_name=str(rule_name),
                check_severity=str(severity), records_checked=int(total_records_val),
                records_failed=int(failures), check_passed=bool(failures == 0)
            ))

            if failures > 0 and str(severity).lower() == "critical":
                has_critical_failures = True

        df_logs = (
            spark.createDataFrame(quality_log_entries)
            .withColumn("executed_at", F.current_timestamp())
        )
        logger.info(f"Guardando logs en {log_path}...")
        write_delta_append(df_logs, log_path, merge_schema=True)

    return has_critical_failures


def handle_quarantine(df_bad, quality_rules, quarantine_output_path, tenant):
    if "tipo_entrega_valido" in quality_rules:
        df_quarantine = df_bad.filter(F.col("_rule_tipo_entrega_valido"))
        df_discard = df_bad.filter(~F.col("_rule_tipo_entrega_valido"))
        logger.info(f"Registros descartados (no persistidos): {df_discard.count()}")
    else:
        df_quarantine = df_bad

    quarantine_rules = [
        r for r, cfg in quality_rules.items()
        if (cfg.severity if hasattr(cfg, 'severity') else cfg.get('severity', 'info')) != "info"
    ]

    if quarantine_rules:
        reasons_expr = F.concat_ws(", ", *[
            F.when((~F.col(f"_rule_{r}")) | F.col(f"_rule_{r}").isNull(), F.lit(r))
            for r in quarantine_rules
        ])
        df_quarantine = df_quarantine.withColumn("_quarantine_reason", reasons_expr)
    else:
        df_quarantine = df_quarantine.withColumn("_quarantine_reason", F.lit("unknown"))

    quarantine_count = df_quarantine.count()
    if quarantine_count > 0:
        logger.warning(f"Se detectaron {quarantine_count} anomalías para cuarentena. Guardando...")
        quarantine_columns = [
            c for c in df_quarantine.columns
            if not c.startswith("_rule_") and c != "fecha_proceso_dt"
        ]
        df_quarantine_final = (
            df_quarantine.select(*quarantine_columns)
            .withColumn("_quarantine_timestamp", F.current_timestamp())
        )

        unique_values = [row["fecha_proceso"] for row in df_quarantine_final.select("fecha_proceso").distinct().collect()]
        partition_conditions = []
        valid_values = [v for v in unique_values if v is not None]

        if valid_values:
            values_sql = ", ".join([f"'{v}'" for v in valid_values])
            partition_conditions.append(f"fecha_proceso IN ({values_sql})")

        if None in unique_values:
            partition_conditions.append("fecha_proceso IS NULL")

        quarantine_replace_cond = " OR ".join(partition_conditions)
        write_delta_replace_where(
            df_quarantine_final, quarantine_output_path, quarantine_replace_cond,
            partition_by=["fecha_proceso"], merge_schema=True
        )
    else:
        logger.info("Capa Silver procesada sin anomalías de cuarentena.")


def process_silver_deliveries(tenant: str, env: str = "dev", start_date: str = None, end_date: str = None):
    conf = load_config(tenant=tenant, env=env)
    spark = get_spark_session()

    bronze_deliveries_path = f"{conf.paths.bronze}/{tenant.lower()}/deliveries"
    silver_materials_path = f"{conf.paths.silver}/global/dim_materials"

    silver_output_path = f"{conf.paths.silver}/{tenant.lower()}/fact_deliveries"
    quarantine_output_path = f"{conf.paths.quarantine_root}/silver_quarantine/{tenant.lower()}/fact_deliveries"

    _run_id = str(uuid.uuid4())

    logger.info(f"Iniciando Capa Silver de Entregas para el tenant: {tenant.upper()}")

    # --- VALIDACIÓN FUERTE DE DEPENDENCIA ---
    if not DeltaTable.isDeltaTable(spark, silver_materials_path):
        error_msg = (
            f"Dependencia incumplida: dim_materials no existe en {silver_materials_path}. "
            "Ejecuta process_materials primero."
        )
        logger.error(error_msg)
        raise RuntimeError(error_msg)

    try:
        df_bronze = (
            spark.read.format("delta").load(bronze_deliveries_path)
            .filter(F.col("_tenant_id") == tenant.lower())
        )

        if start_date:
            df_bronze = df_bronze.filter(F.col("fecha_proceso").cast("string") >= start_date.replace("-", ""))
        if end_date:
            df_bronze = df_bronze.filter(F.col("fecha_proceso").cast("string") <= end_date.replace("-", ""))

        df_bronze = df_bronze.dropDuplicates()
        df_materials = spark.read.format("delta").load(silver_materials_path)
    except Exception as e:
        logger.error(f"Error al leer orígenes en Silver: {e}")
        return

    df_transformed = apply_deliveries_transformations(df_bronze)
    df_joined = apply_scd2_join(df_transformed, df_materials)

    quality_rules = conf.schemas.deliveries.silver.quality_rules
    df_evaluated, is_apt_for_silver_cond = evaluate_quality_rules(df_joined, quality_rules)

    quality_logs_path = f"{conf.paths.shared}/quality_logs"
    has_critical_failures = record_quality_logs(
        spark, df_evaluated, quality_rules, _run_id, tenant, quality_logs_path
    )

    df_silver_final = (
        df_evaluated.filter(is_apt_for_silver_cond)  # Solo registros que cumplen todas las reglas de calidad
        .withColumn("_silver_timestamp", F.current_timestamp())
    )
    df_bad = df_evaluated.filter(~is_apt_for_silver_cond)  # Registros que no cumplen alguna regla de calidad, para análisis y cuarentena

    # --- ORDENAMIENTO ESTÁNDAR DE COLUMNAS ---
    cols_llaves = ["_tenant_id", "fecha_proceso", "transporte", "ruta", "material", "tipo_entrega"]
    cols_auditoria = [c for c in df_silver_final.columns if c.startswith("_") and not c.startswith("_rule_") and c not in cols_llaves]
    cols_metricas = [
        c for c in df_silver_final.columns
        if c not in cols_llaves + cols_auditoria and not c.startswith("_rule_") and c != "fecha_proceso_dt"
    ]

    df_silver_clean = df_silver_final.select(*(cols_llaves + cols_metricas + cols_auditoria))

    logger.info("Escribiendo datos procesados en fact_deliveries mediante MERGE...")
    merge_condition = (
        "target._tenant_id = source._tenant_id AND target.fecha_proceso = source.fecha_proceso AND "
        "target.transporte = source.transporte AND target.ruta = source.ruta AND "
        "target.material = source.material AND target.tipo_entrega = source.tipo_entrega"
    )
    write_delta_merge(
        spark, df_silver_clean, silver_output_path, merge_condition, ["fecha_proceso"]
    )

    handle_quarantine(df_bad, quality_rules, quarantine_output_path, tenant)

    fail_on_critical = conf.get("quality", {}).get("fail_on_critical", False)
    if has_critical_failures and fail_on_critical:
        error_msg = "Se detectaron errores CRÍTICOS de calidad. Abortando el pipeline."
        logger.error(error_msg)
        raise ValueError(error_msg)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Procesamiento Capa Silver - Entregas")
    parser.add_argument("--tenant", type=str, required=True, help="Código del tenant (ej. ec)")
    parser.add_argument("--env", type=str, default="dev", help="Entorno de ejecución (dev, qa, main)")
    parser.add_argument("--start-date", type=str, help="Fecha de inicio (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="Fecha de fin (YYYY-MM-DD)")
    args = parser.parse_args()
    process_silver_deliveries(tenant=args.tenant, env=args.env, start_date=args.start_date, end_date=args.end_date)
