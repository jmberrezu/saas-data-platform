import argparse
import logging
import sys

from src.saas_pipeline.bronze.bronze import process_bronze
from src.saas_pipeline.config import load_config
from src.saas_pipeline.gold.daily_metrics_by_delivery_type import process_gold_daily_metrics
from src.saas_pipeline.silver.fact_deliveries import process_silver_deliveries
from src.saas_pipeline.silver.dim_materials import process_silver_materials

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def run_pipeline(tenant_arg: str, env: str = "dev", start_date: str = None, end_date: str = None):
    logger.info("=" * 60)
    logger.info(f" INICIANDO ORQUESTACIÓN (TENANT: {tenant_arg.upper()} | ENV: {env.upper()})")
    logger.info("=" * 60)

    conf = load_config(tenant="base", env=env)
    fail_fast = conf.get("execution", {}).get("fail_fast", False)
    active_tenants = conf.get("execution", {}).get("tenants", [])

    tenants_to_process = active_tenants if tenant_arg == "all" else [tenant_arg]

    # 1. Catálogos Globales (Solo se procesan una vez)
    try:
        logger.info("\n--- PROCESANDO CATÁLOGOS GLOBALES ---")
        process_bronze(tenant="global", entity="materials", env=env)
        process_silver_materials(env=env)
    except Exception as e:
        logger.error(f" Error crítico en catálogos globales: {e}")
        if fail_fast:
            sys.exit(1)

    errors = {}

    # 2. Tablas Transaccionales por Tenant
    for t in tenants_to_process:
        logger.info("\n" + "=" * 40)
        logger.info(f" PROCESANDO TENANT: {t.upper()}")
        logger.info("=" * 40)

        try:
            logger.info("\n--- PASO 1: CAPA BRONZE ---")
            process_bronze(tenant=t, entity="deliveries", env=env, start_date=start_date, end_date=end_date)

            logger.info("\n--- PASO 2: CAPA SILVER ---")
            process_silver_deliveries(tenant=t, env=env, start_date=start_date, end_date=end_date)

            logger.info("\n--- PASO 3: CAPA GOLD ---")
            process_gold_daily_metrics(tenant=t, env=env, start_date=start_date, end_date=end_date)

        except Exception as e:
            logger.error(f" Error procesando tenant {t}: {e}")
            errors[t] = str(e)
            if fail_fast:
                logger.error(" Abortando pipeline debido a fail_fast=True")
                sys.exit(1)

    if errors:
        logger.warning(f"\n PIPELINE COMPLETADO CON ERRORES EN: {list(errors.keys())}")
    else:
        logger.info("\n PIPELINE COMPLETADO EXITOSAMENTE PARA TODOS LOS TENANTS")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Orquestador del Pipeline SAAS")
    parser.add_argument(
        "--tenant",
        type=str,
        default="all",
        help="Código del tenant (ej. ec, sv) o 'all' para procesar todos"
    )
    parser.add_argument("--env", type=str, default="dev", help="Entorno de ejecución (dev, qa, main)")
    parser.add_argument("--start-date", type=str, help="Fecha de inicio (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="Fecha de fin (YYYY-MM-DD)")
    args = parser.parse_args()

    run_pipeline(args.tenant.lower(), args.env, args.start_date, args.end_date)
