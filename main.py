"""
Pipeline Receita Federal - Dados CNPJ

Pipeline completo de ingestão e processamento de dados públicos da Receita Federal
utilizando arquitetura Medallion (RAW -> Bronze -> Silver -> Gold).

Architecture
------------
RAW Layer (landing): Download e extração de arquivos da Receita Federal
Bronze Layer: Ingestão CSV para Delta Lake (preservação de dados brutos)
Silver Layer: Limpeza, validação e conversão de tipos
Gold Layer: Agregações e regras de negócio (output final)

Execution
---------
docker-compose up

Notes
-----
Pipeline é executado sequencialmente. Falha em qualquer camada
interrompe execução para garantir integridade dos dados.
"""

import logging
from pipelines.raw_rf_cnpj import executar_camada_raw
from pipelines.bronze_rf_cnpj import executar_camada_bronze
from pipelines.silver_rf_cnpj import executar_camada_silver
from pipelines.gold_rf_cnpj import executar_camada_gold

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def main() -> None:
    """
    Executa pipeline completo de processamento de dados CNPJ.
    
    Notes
    -----
    Pipeline executa sequencialmente:
    1. RAW: Download de arquivos da Receita Federal
    2. Bronze: Conversão CSV -> Delta Lake
    3. Silver: Limpeza e validação
    4. Gold: Agregações e output final
    
    Falha em qualquer camada interrompe o pipeline.
    """
    logger.info("=" * 60)
    logger.info("Iniciando pipeline para ingestão de dados de CNPJ da Receita Federal")
    logger.info("=" * 60)

    # RAW Layer
    if not executar_camada_raw():
        logger.error("Falha na RAW Layer")
        exit(1)

    # Bronze Layer
    if not executar_camada_bronze():
        logger.error("Falha na Bronze Layer")
        exit(1)
    
    # Silver Layer
    if not executar_camada_silver():
        logger.error("Falha na Silver Layer")
        exit(1)
    
    # Gold Layer
    if not executar_camada_gold():
        logger.error("Falha na Gold Layer")
        exit(1)
    
    logger.info("=" * 60)
    logger.info("Pipeline concluído com sucesso")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()