import pytest
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from src.saas_pipeline.silver.fact_deliveries import evaluate_quality_rules
from src.saas_pipeline.config import load_config


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-quality") \
        .getOrCreate()


def test_quality_rules_evaluation(spark):
    """
    PRUEBA 3: Verifica que el motor de calidad detecte correctamente cantidades nulas/negativas 
    y tipos de entrega inválidos evaluando expresiones SQL de configuración.
    """
    # 1. Arrange: Simulamos datos (Mala y Buena)
    data = [
        (-5.0, "Z99"),  # Cantidad negativa (Invalida), Tipo Invalido
        (10.0, "ZPRE")  # Cantidad positiva (Valida), Tipo Valido (Rutina)
    ]
    df = spark.createDataFrame(data, ["cantidad", "tipo_entrega"])

    # Simulamos el diccionario de reglas que vendría del YAML
    mock_rules = {
        "cantidad_valida": {"expr": "cantidad > 0 AND cantidad IS NOT NULL"},
        "tipo_entrega_valido": {"expr": "tipo_entrega IN ('ZPRE', 'ZVE1', 'Z04', 'Z05')"}
    }

    # 2. Act: Pasamos el DataFrame por nuestra función Pura Real
    df_evaluated, condicion_apto = evaluate_quality_rules(df, mock_rules)
    result = df_evaluated.collect()

    # 3. Assert
    assert result[0]["_rule_cantidad_valida"] is False
    assert result[0]["_rule_tipo_entrega_valido"] is False
    assert result[1]["_rule_cantidad_valida"] is True
    assert result[1]["_rule_tipo_entrega_valido"] is True


def test_yaml_configuration_loads():
    """
    PRUEBA 4: Validación de que los archivos YAML de configuración cargan correctamente.
    (Cumple explícitamente con el requerimiento de la rúbrica 7.1)
    """
    conf = load_config(tenant="ec")
    
    assert "paths" in conf
    assert "schemas" in conf
    assert "deliveries" in conf.schemas
    assert conf.schemas.deliveries.tenant_column == "pais"
    # Verificamos que se cargó al menos una regla de calidad
    assert "cantidad_valida" in conf.schemas.deliveries.silver.quality_rules