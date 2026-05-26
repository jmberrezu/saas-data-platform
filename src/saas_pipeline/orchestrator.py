import argparse
import logging
import sys

from src.saas_pipeline.bronze.bronze import process_bronze
from src.saas_pipeline.config import load_config
from src.saas_pipeline.gold import process_gold_daily_metrics
from src.saas_pipeline.playground import main as run_playground
from src.saas_pipeline.silver.fact_deliveries import process_silver_deliveries
from src.saas_pipeline.silver.dim_materials import process_silver_materials

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ALL_TENANTS = ["ec", "sv", "gt", "hn", "cr", "ni"]


def run_pipeline(tenant_arg: str):
    logger.info("="*60)
    logger.info(f" INICIANDO ORQUESTACIÓN (TENANT: {tenant_arg.upper()})")
    logger.info("="*60)

    conf = load_config(tenant="base")
    fail_fast = conf.get("execution", {}).get("fail_fast", False)

    tenants_to_process = ALL_TENANTS if tenant_arg == "all" else [tenant_arg]

    # 1. Catálogos Globales (Solo se procesan una vez)
    try:
        logger.info("\n--- PROCESANDO CATÁLOGOS GLOBALES ---")
        process_bronze(tenant="global", entity="materials")
        process_silver_materials()
    except Exception as e:
        logger.error(f" Error crítico en catálogos globales: {e}")
        if fail_fast:
            sys.exit(1)

    errors = {}

    # 2. Tablas Transaccionales por Tenant
    for t in tenants_to_process:
        logger.info("\n" + "="*40)
        logger.info(f" PROCESANDO TENANT: {t.upper()}")
        logger.info("="*40)

        try:
            logger.info("\n--- PASO 1: CAPA BRONZE ---")
            process_bronze(tenant=t, entity="deliveries")

            logger.info("\n--- PASO 2: CAPA SILVER ---")
            process_silver_deliveries(tenant=t)

            logger.info("\n--- PASO 3: CAPA GOLD ---")
            process_gold_daily_metrics(tenant=t)

        except Exception as e:
            logger.error(f" Error procesando tenant {t}: {e}")
            errors[t] = str(e)
            if fail_fast:
                logger.error(" Abortando pipeline debido a fail_fast=True")
                sys.exit(1)

    logger.info("\n--- EJECUTANDO AUDITORÍA ---")
    run_playground()

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
    args = parser.parse_args()

    run_pipeline(args.tenant.lower())
