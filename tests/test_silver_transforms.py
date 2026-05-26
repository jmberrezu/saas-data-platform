import pytest
from pyspark.sql import SparkSession
from datetime import date
from src.saas_pipeline.silver.fact_deliveries import apply_deliveries_transformations, apply_scd2_join


@pytest.fixture(scope="session")
def spark():
    """Crea una sesión de Spark local optimizada para pruebas."""
    return SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-pyspark-local-testing") \
        .getOrCreate()


def test_conversion_unidades_y_flags(spark):
    """
    PRUEBA 1: Verifica la normalización de unidades (1 CS = 20 ST)
    y el mapeo correcto de los flags de tipo de entrega.
    """
    # 1. Arrange: Datos simulados crudos
    data = [
        ("CS", 5.0, "ZPRE", 20250115),  # Cajas, rutina, con fecha
        ("ST", 10.0, "Z04", 20250116),  # Unidades sueltas, bonificación, con fecha
    ]
    df_raw = spark.createDataFrame(data, ["unidad", "cantidad", "tipo_entrega", "fecha_proceso"])

    # 2. Act: Usamos la función REAL importada desde nuestro pipeline productivo
    df_transformed = apply_deliveries_transformations(df_raw)
    result = df_transformed.collect()

    # 3. Assert: Verificaciones
    # Registro 1 (5 CS -> 100 ST, rutina)
    assert result[0]["cantidad_normalizada_st"] == 100.0
    assert result[0]["is_routine_delivery"] is True
    assert result[0]["is_bonus_delivery"] is False

    # Registro 2 (10 ST -> 10 ST, bonificación)
    assert result[1]["cantidad_normalizada_st"] == 10.0
    assert result[1]["is_routine_delivery"] is False
    assert result[1]["is_bonus_delivery"] is True


def test_scd2_temporal_join(spark):
    """
    PRUEBA 2: Verifica que una transacción cruce con la versión histórica correcta del catálogo (SCD2).
    """
    # 1. Arrange: Un material que cambió de precio en febrero
    dim_data = [
        ("SKU1", 10.0, date(2025, 1, 1), date(2025, 1, 31)),  # Precio viejo
        ("SKU1", 20.0, date(2025, 2, 1), date(9999, 12, 31))  # Precio nuevo
    ]
    df_dim = spark.createDataFrame(dim_data, ["material", "precio_base", "valid_from", "valid_to"])

    # Una entrega que ocurrió el 15 de enero (debe traer el precio de 10.0)
    fact_data = [("SKU1", date(2025, 1, 15))]
    df_fact = spark.createDataFrame(fact_data, ["material", "fecha_proceso_dt"])

    # 2. Act: Join Temporal
    df_joined = apply_scd2_join(df_fact, df_dim)
    result = df_joined.collect()

    # 3. Assert: Aseguramos que cruzó y trajo el precio histórico, no el actual
    assert len(result) == 1
    assert result[0]["precio_catalogo_unitario"] == 10.0
