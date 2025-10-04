"""
GOLD Layer - Agregações e Regras de Negócio

Responsabilidades:
- Agregar dados limpos da Silver
- Aplicar regras de negócio conforme requisitos
- Gerar tabela analítica (OBT - One Big Table)
- Persistir em Delta Lake + PostgreSQL

Regras de Negócio:
- qtde_socios: COUNT de sócios por CNPJ
- flag_socio_estrangeiro: True se ≥1 sócio estrangeiro
- doc_alvo: True se cod_porte='03' AND qtde_socios>1

Idempotência:
- Mode overwrite em Delta e PostgreSQL
- Múltiplas execuções = mesmo resultado
"""

import logging
import os
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# ==============================================================================
# Configurações
# ==============================================================================

DATA_PATH = Path("/opt/data")
SILVER_PATH = DATA_PATH / "silver"
GOLD_PATH = DATA_PATH / "gold"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ==============================================================================
# Spark Session
# ==============================================================================

def criar_spark_session() -> SparkSession:
    """Cria SparkSession com configurações Delta Lake."""
    return (
        SparkSession.builder
        .appName("RF-CNPJ-Gold")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

# ==============================================================================
# Transformações Gold
# ==============================================================================

def criar_tabela_analitica(spark: SparkSession):
    """
    Cria tabela analítica com agregações e regras de negócio.
    
    Query SQL:
    - Filtra APENAS registros ativos (ativo=True) para SCD Type 2
    - JOIN empresas + sócios (INNER: apenas CNPJs com sócios)
    - GROUP BY cnpj, cod_porte
    - Agregações: COUNT sócios, MAX flag estrangeiro
    - Flag calculada: doc_alvo
    
    Regras de Negócio:
    1. qtde_socios = COUNT de registros de sócios
    2. flag_socio_estrangeiro = True se pelo menos 1 sócio tem is_estrangeiro=True
    3. doc_alvo = True se cod_porte='03' (Empresa de Grande Porte) E qtde_socios > 1
    
    SCD Type 2:
    - WHERE ativo = True garante usar apenas versão atual das empresas
    
    Returns:
        DataFrame com colunas: cnpj, qtde_socios, flag_socio_estrangeiro, doc_alvo
    """
    empresas_df = spark.read.format("delta").load(str(SILVER_PATH / "empresas"))
    socios_df = spark.read.format("delta").load(str(SILVER_PATH / "socios"))
    
    # Registrar como views temporárias para SQL
    empresas_df.createOrReplaceTempView("empresas")
    socios_df.createOrReplaceTempView("socios")
    
    # Query analítica - FILTRA APENAS REGISTROS ATIVOS
    df_resultado = spark.sql("""
        SELECT 
            e.cnpj,
            COUNT(s.cnpj) AS qtde_socios,
            CAST(MAX(CASE WHEN s.is_estrangeiro THEN 1 ELSE 0 END) AS BOOLEAN) 
                AS flag_socio_estrangeiro,
            CAST((e.cod_porte = '03' AND COUNT(s.cnpj) > 1) AS BOOLEAN) 
                AS doc_alvo
        FROM empresas e
        INNER JOIN socios s ON e.cnpj = s.cnpj
        WHERE e.ativo = True
        GROUP BY e.cnpj, e.cod_porte
    """)
    
    return df_resultado

# ==============================================================================
# Persistência
# ==============================================================================

def salvar_delta(df, path: Path) -> None:
    """Salva DataFrame em Delta Lake (mode overwrite)."""
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(path))
    logger.info(f"Delta Lake salvo: {path.name} ({df.count():,} registros)")

def salvar_postgres(df, table_name: str) -> None:
    """
    Salva DataFrame no PostgreSQL (mode overwrite).
    
    Conversões:
    - BOOLEAN → INTEGER (0/1) para compatibilidade PostgreSQL
    """
    # Converter BOOLEAN para INTEGER (PostgreSQL compatibility)
    df_postgres = df.select(
        col("cnpj"),
        col("qtde_socios"),
        when(col("flag_socio_estrangeiro"), 1).otherwise(0).alias("flag_socio_estrangeiro"),
        when(col("doc_alvo"), 1).otherwise(0).alias("doc_alvo")
    )
    
    # Configuração JDBC
    jdbc_url = (
        f"jdbc:postgresql://"
        f"{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/"
        f"{os.getenv('POSTGRES_DB')}"
    )
    
    jdbc_props = {
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "driver": "org.postgresql.Driver"
    }
    
    # Escrever no PostgreSQL
    df_postgres.write.jdbc(
        url=jdbc_url,
        table=table_name,
        mode="overwrite",
        properties=jdbc_props
    )
    logger.info(f"PostgreSQL salvo: {table_name} ({df_postgres.count():,} registros)")

# ==============================================================================
# Orquestração
# ==============================================================================

def executar_camada_gold() -> bool:
    """
    Executa pipeline completo da camada Gold.
    
    Pipeline:
    1. Ler Silver (empresas + socios ativos)
    2. Aplicar agregações e regras de negócio
    3. Salvar em Delta Lake
    4. Salvar em PostgreSQL
    
    Idempotência:
    - Mode overwrite garante mesmo resultado em múltiplas execuções
    - Não depende de estado anterior
    
    Returns:
        True se sucesso, False se erro
    """
    GOLD_PATH.mkdir(parents=True, exist_ok=True)
    spark = criar_spark_session()
    
    try:
        logger.info("=" * 60)
        logger.info("Iniciando Gold Layer")
        logger.info("=" * 60)
        
        # 1. Criar tabela analítica
        df_resultado = criar_tabela_analitica(spark)
        
        # 2. Salvar Delta Lake
        salvar_delta(df_resultado, GOLD_PATH / "df_resultado_final")
        
        # 3. Salvar PostgreSQL
        salvar_postgres(df_resultado, "df_resultado_final")
        
        logger.info("=" * 60)
        logger.info("Gold Layer concluída com sucesso")
        logger.info("=" * 60)
        return True
        
    except Exception as e:
        logger.error(f"Erro na Gold Layer: {e}")
        return False
    finally:
        spark.stop()

if __name__ == "__main__":
    exit(0 if executar_camada_gold() else 1)