"""
BRONZE Layer - Ingestão CSV para Delta Lake

Dados brutos preservados sem transformações.
Schema: todas colunas STRING.
"""

import logging
import json
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import lit

DATA_PATH = Path("/opt/data")
RAW_PATH = DATA_PATH / "raw"
BRONZE_PATH = DATA_PATH / "bronze"
METADATA_PATH = RAW_PATH / "metadata.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Schemas
SCHEMA_EMPRESAS = StructType([
    StructField("cnpj", StringType(), True),
    StructField("razao_social", StringType(), True),
    StructField("natureza_juridica", StringType(), True),
    StructField("qualificacao_responsavel", StringType(), True),
    StructField("capital_social", StringType(), True),
    StructField("cod_porte", StringType(), True),
])

SCHEMA_SOCIOS = StructType([
    StructField("cnpj", StringType(), True),
    StructField("tipo_socio", StringType(), True),
    StructField("nome_socio", StringType(), True),
    StructField("documento_socio", StringType(), True),
    StructField("codigo_qualificacao_socio", StringType(), True),
])


def criar_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("RF-Bronze")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def carregar_metadata() -> dict:
    if METADATA_PATH.exists():
        return json.loads(METADATA_PATH.read_text())
    return {
        "data_referencia": datetime.now().strftime("%Y-%m"),
        "versao_dados": datetime.now().strftime("%Y-%m-%d")
    }


def processar_csv(spark, csv_path, schema, nome, metadata):
    """Lê CSV e salva no Delta (dados brutos)."""
    logger.info(f"Processando: {nome}")
    
    # Ler CSV
    df = spark.read \
        .option("sep", ";") \
        .option("encoding", "ISO-8859-1") \
        .option("header", "false") \
        .schema(schema) \
        .csv(str(csv_path))
    
    total = df.count()
    logger.info(f"{nome}: {total:,} registros")
    
    if total == 0:
        logger.error(f"{nome}: CSV vazio")
        return False
    
    # Adicionar metadados mínimos
    df_final = df \
        .withColumn("_data_referencia", lit(metadata.get("data_referencia"))) \
        .withColumn("_versao_dados", lit(metadata.get("versao_dados"))) \
        .withColumn("_data_processamento", lit(datetime.now().isoformat()))
    
    # Salvar (overwrite - dados brutos)
    delta_path = BRONZE_PATH / nome
    df_final.write.format("delta").mode("overwrite").save(str(delta_path))
    
    logger.info(f"{nome}: Salvo no Delta")
    return True


def limpar_raw():
    total_mb = 0
    for arquivo in list(RAW_PATH.glob("*.zip")) + list(RAW_PATH.glob("*.CSV")):
        try:
            total_mb += arquivo.stat().st_size / (1024 * 1024)
            arquivo.unlink()
        except:
            pass
    if total_mb > 0:
        logger.info(f"FinOps: {total_mb:.1f} MB liberados")


def executar_camada_bronze() -> bool:
    logger.info("Iniciando Bronze Layer")
    
    BRONZE_PATH.mkdir(parents=True, exist_ok=True)
    metadata = carregar_metadata()
    spark = criar_spark()
    
    try:
        empresas_csv = next(RAW_PATH.rglob("*EMPRE*CSV"))
        socios_csv = next(RAW_PATH.rglob("*SOCIO*CSV"))
        
        empresas_ok = processar_csv(spark, empresas_csv, SCHEMA_EMPRESAS, "empresas", metadata)
        socios_ok = processar_csv(spark, socios_csv, SCHEMA_SOCIOS, "socios", metadata)
        
        if empresas_ok and socios_ok:
            logger.info(f"Bronze concluída - Versão: {metadata.get('versao_dados')}")
            limpar_raw()
            return True
        else:
            logger.error("Falha na Bronze")
            return False
        
    except StopIteration:
        logger.error("CSVs não encontrados")
        return False
    except Exception as e:
        logger.error(f"Erro: {e}")
        return False
    finally:
        spark.stop()


if __name__ == "__main__":
    exit(0 if executar_camada_bronze() else 1)