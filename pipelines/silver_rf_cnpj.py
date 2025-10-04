"""
SILVER Layer - Limpeza, Validação e SCD Type 2

Arquitetura Medalhão:
- Bronze: Dados brutos (raw)
- Silver: Dados limpos e validados com SCD Type 2
- Gold: Dados agregados para consumo
"""

import logging
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, length, trim, upper, lit, current_timestamp,
    concat_ws, sha2, substring, concat
)
from pyspark.sql.types import StringType, IntegerType, FloatType, BooleanType
from delta.tables import DeltaTable

# Configurações
DATA_PATH = Path("/opt/data")
BRONZE_PATH = DATA_PATH / "bronze"
SILVER_PATH = DATA_PATH / "silver"
MASCARAR_DOCUMENTOS = True

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ==============================================================================
# Spark Session
# ==============================================================================

def criar_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("RF-CNPJ-Silver")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

# ==============================================================================
# Limpeza e Validação
# ==============================================================================

def limpar_empresas(df: DataFrame) -> DataFrame:
    """Valida e normaliza empresas."""
    return (
        df
        .filter(col("cnpj").isNotNull() & (length(col("cnpj")) == 8))
        .filter(col("razao_social").isNotNull())
        .select(
            col("cnpj").cast(StringType()),
            upper(trim(col("razao_social"))).alias("razao_social"),
            col("natureza_juridica").cast(IntegerType()),
            col("qualificacao_responsavel").cast(IntegerType()),
            when(col("capital_social").isNull(), 0.0)
                .otherwise(col("capital_social")).cast(FloatType()).alias("capital_social"),
            when(col("cod_porte").isNull(), "99")
                .otherwise(col("cod_porte")).cast(StringType()).alias("cod_porte")
        )
        .dropDuplicates(["cnpj"])
    )


def limpar_socios(df: DataFrame) -> DataFrame:
    """Valida e normaliza sócios."""
    
    # Máscara de documento (LGPD)
    doc_mask = (
        when(col("documento_socio").rlike("^\\*\\*\\*"), col("documento_socio"))
        .when(col("documento_socio").isNull(), lit(None))
        .otherwise(concat(lit("***"), substring(col("documento_socio"), -6, 6), lit("**")))
    ) if MASCARAR_DOCUMENTOS else col("documento_socio")
    
    return (
        df
        .filter(col("cnpj").isNotNull() & (length(col("cnpj")) == 8))
        .select(
            col("cnpj").cast(StringType()),
            col("tipo_socio").cast(IntegerType()),
            when(col("nome_socio").isNull(), "NAO INFORMADO")
                .otherwise(upper(trim(col("nome_socio")))).alias("nome_socio"),
            doc_mask.cast(StringType()).alias("documento_socio"),
            col("codigo_qualificacao_socio").cast(StringType()),
            when(col("documento_socio").rlike("^9+$"), True)
                .otherwise(False).cast(BooleanType()).alias("is_estrangeiro")
        )
        .dropDuplicates(["cnpj", "tipo_socio", "nome_socio", "documento_socio"])
    )

# ==============================================================================
# SCD Type 2 - Slowly Changing Dimensions
# ==============================================================================

def aplicar_scd_type2(df_novo: DataFrame, spark: SparkSession, path: Path):
    """
    SCD Type 2: Rastreia mudanças históricas
    
    Colunas de controle:
    - data_inicio: quando o registro ficou ativo
    - data_fim: quando foi substituído (NULL = ativo)
    - ativo: True = versão atual, False = histórico
    - hash_chave: detecta mudanças nos dados
    """
    
    # Hash para detectar mudanças
    df_novo = df_novo.withColumn(
        "hash_chave",
        sha2(concat_ws("||", "razao_social", "natureza_juridica", 
                       "qualificacao_responsavel", "capital_social", "cod_porte"), 256)
    )
    
    # Primeira carga - tabela não existe
    if not DeltaTable.isDeltaTable(spark, str(path)):
        logger.info("SCD Type 2: Primeira carga")
        (
            df_novo
            .withColumn("data_inicio", current_timestamp())
            .withColumn("data_fim", lit(None).cast("timestamp"))
            .withColumn("ativo", lit(True))
            .write.format("delta").mode("overwrite").save(str(path))
        )
        return
    
    # Cargas incrementais
    logger.info("SCD Type 2: Carga incremental")
    delta_table = DeltaTable.forPath(spark, str(path))
    
    # Etapa 1: Fechar registros que mudaram (ativo=True -> False)
    delta_table.alias("antigo").merge(
        df_novo.alias("novo"),
        "antigo.cnpj = novo.cnpj AND antigo.ativo = True"
    ).whenMatchedUpdate(
        condition="antigo.hash_chave != novo.hash_chave",
        set={
            "data_fim": current_timestamp(),
            "ativo": lit(False)
        }
    ).execute()
    
    # Etapa 2: Inserir registros novos e alterados
    # Anti-join: pega tudo de df_novo que NÃO está ativo na tabela
    df_para_inserir = (
        df_novo.alias("n")
        .join(
            delta_table.toDF().filter(col("ativo")).alias("a"),
            on="cnpj",
            how="left_anti"
        )
        .select("n.*")
        .withColumn("data_inicio", current_timestamp())
        .withColumn("data_fim", lit(None).cast("timestamp"))
        .withColumn("ativo", lit(True))
    )
    
    # Append dos novos registros
    if df_para_inserir.count() > 0:
        df_para_inserir.write.format("delta").mode("append").save(str(path))
        logger.info(f"SCD Type 2: {df_para_inserir.count()} registros inseridos")
    else:
        logger.info("SCD Type 2: Nenhum registro novo")

# ==============================================================================
# Orquestração
# ==============================================================================

def processar_empresas(spark: SparkSession):
    """Processa empresas com SCD Type 2."""
    logger.info("Processando EMPRESAS com SCD Type 2")
    
    # Bronze -> Silver
    df_bronze = spark.read.format("delta").load(str(BRONZE_PATH / "empresas"))
    df_silver = limpar_empresas(df_bronze)
    
    # Aplica SCD Type 2
    silver_path = SILVER_PATH / "empresas"
    aplicar_scd_type2(df_silver, spark, silver_path)
    
    # Otimiza
    spark.sql(f"OPTIMIZE delta.`{silver_path}`")
    logger.info("Empresas: OK")


def processar_socios(spark: SparkSession):
    """Processa sócios (snapshot simples)."""
    logger.info("Processando SOCIOS (snapshot)")
    
    # Bronze -> Silver
    df_bronze = spark.read.format("delta").load(str(BRONZE_PATH / "socios"))
    df_silver = limpar_socios(df_bronze)
    
    # Overwrite simples (sem SCD Type 2)
    silver_path = SILVER_PATH / "socios"
    df_silver.write.format("delta").mode("overwrite").save(str(silver_path))
    
    # Otimiza
    spark.sql(f"OPTIMIZE delta.`{silver_path}`")
    logger.info("Socios: OK")


def executar_camada_silver():
    """Pipeline Silver completo."""
    SILVER_PATH.mkdir(parents=True, exist_ok=True)
    
    logger.info("="*60)
    logger.info("CAMADA SILVER - SCD Type 2 para Empresas")
    logger.info("="*60)
    
    spark = criar_spark_session()
    
    try:
        processar_empresas(spark)
        processar_socios(spark)
        
        logger.info("="*60)
        logger.info("RESULTADO: SUCESSO")
        logger.info("="*60)
        return True
        
    except Exception as e:
        logger.error(f"ERRO: {e}")
        return False
        
    finally:
        spark.stop()


if __name__ == "__main__":
    exit(0 if executar_camada_silver() else 1)