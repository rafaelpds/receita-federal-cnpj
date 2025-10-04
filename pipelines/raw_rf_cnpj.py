"""
RAW Layer - Download de dados da Receita Federal

Camada RAW (Landing Zone) do modelo Medallion Architecture.

Responsabilidades:
- Download de arquivos ZIP da Receita Federal
- Extração de CSVs para pasta RAW
- Captura de versão (Last-Modified) para versionamento futuro
- Salvar metadata mínimo para Bronze

Dependencies:
- requests>=2.31.0
"""

import logging
import requests
import zipfile
import json
from pathlib import Path
from datetime import datetime
from typing import Dict

# ============================================================================
# CONFIGURAÇÕES
# ============================================================================
DATA_REFERENCIA = "2025-09"  # 🔧 Ajustar aqui para mudar período
BASE_URL = f"https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/{DATA_REFERENCIA}/"
DATA_PATH = Path("/opt/data")
RAW_PATH = DATA_PATH / "raw"
METADATA_PATH = RAW_PATH / "metadata.json"
FILES_TO_DOWNLOAD = ["Empresas1.zip", "Socios1.zip"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# ============================================================================
# FUNÇÕES
# ============================================================================

def capturar_versao_arquivo(url: str) -> str:
    """
    Captura versão do arquivo (Last-Modified ou data atual).
    
    Parameters
    ----------
    url : str
        URL do arquivo
    
    Returns
    -------
    str
        Versão no formato YYYY-MM-DD
        
    Notes
    -----
    Last-Modified identifica quando RF gerou/atualizou o arquivo.
    Se não disponível, usa data atual como fallback.
    """
    try:
        response = requests.head(url, timeout=30, allow_redirects=True)
        response.raise_for_status()
        
        last_modified = response.headers.get("Last-Modified")
        
        if last_modified:
            dt = datetime.strptime(last_modified, "%a, %d %b %Y %H:%M:%S %Z")
            versao = dt.strftime("%Y-%m-%d")
            logger.info(f"✅ Versão: {versao}")
            return versao
        else:
            logger.warning("Last-Modified não disponível, usando data atual")
            return datetime.now().strftime("%Y-%m-%d")
            
    except Exception as e:
        logger.warning(f"Erro ao capturar versão: {e}, usando data atual")
        return datetime.now().strftime("%Y-%m-%d")


def setup_directories() -> None:
    """Cria estrutura de diretórios."""
    RAW_PATH.mkdir(parents=True, exist_ok=True)
    logger.info("📁 Diretórios RAW configurados")


def download_and_extract(filename: str, metadata: Dict) -> bool:
    """
    Baixa e extrai arquivo ZIP.
    
    Parameters
    ----------
    filename : str
        Nome do arquivo (ex: Empresas1.zip)
    metadata : dict
        Dicionário para armazenar metadados
    
    Returns
    -------
    bool
        True se sucesso
    """
    url = f"{BASE_URL}{filename}"
    zip_path = RAW_PATH / filename

    try:
        logger.info(f"{'='*60}")
        logger.info(f"📦 {filename}")
        logger.info(f"{'='*60}")
        
        # Capturar versão
        versao = capturar_versao_arquivo(url)
        
        metadata["arquivos"][filename] = {
            "url": url,
            "versao_dados": versao,
            "data_download": datetime.now().isoformat()
        }
        
        # Download
        if not zip_path.exists():
            logger.info(f"⬇️  Baixando...")
            response = requests.get(url, timeout=300)
            response.raise_for_status()
            
            zip_path.write_bytes(response.content)
            
            tamanho_mb = zip_path.stat().st_size / (1024 * 1024)
            metadata["arquivos"][filename]["tamanho_mb"] = round(tamanho_mb, 2)
            
            logger.info(f"✅ Download concluído: {tamanho_mb:.1f} MB")
        else:
            logger.info(f"📄 Arquivo já existe localmente")
            tamanho_mb = zip_path.stat().st_size / (1024 * 1024)
            metadata["arquivos"][filename]["tamanho_mb"] = round(tamanho_mb, 2)
        
        # Extração
        logger.info(f"📂 Extraindo...")
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            csvs = zip_ref.namelist()
            zip_ref.extractall(RAW_PATH)
            metadata["arquivos"][filename]["csvs_extraidos"] = csvs
        
        logger.info(f"✅ {len(csvs)} arquivo(s) extraído(s)")
        return True

    except Exception as e:
        logger.error(f"❌ Erro: {e}")
        metadata["arquivos"][filename]["erro"] = str(e)
        return False


def salvar_metadados(metadata: Dict) -> None:
    """Salva metadados em JSON."""
    METADATA_PATH.write_text(json.dumps(metadata, indent=2, ensure_ascii=False))
    logger.info(f"💾 Metadados salvos: {METADATA_PATH}")


def executar_camada_raw() -> bool:
    """
    Executa pipeline RAW.
    
    Returns
    -------
    bool
        True se sucesso
        
    Notes
    -----
    Pipeline:
    1. Download de arquivos ZIP
    2. Extração de CSVs
    3. Captura de versão (Last-Modified)
    4. Salva metadata para Bronze usar
    """
    logger.info("=" * 80)
    logger.info("🚀 RAW LAYER")
    logger.info("=" * 80)
    
    setup_directories()
    
    logger.info(f"📅 Data de referência: {DATA_REFERENCIA}")
    
    # Metadata básico
    metadata = {
        "data_referencia": DATA_REFERENCIA,
        "url_base": BASE_URL,
        "data_execucao": datetime.now().isoformat(),
        "arquivos": {}
    }
    
    # Downloads
    logger.info("\n📦 Iniciando downloads...")
    success = all(download_and_extract(f, metadata) for f in FILES_TO_DOWNLOAD)
    
    if not success:
        logger.error("❌ Falha no download")
        metadata["status"] = "falha"
        salvar_metadados(metadata)
        return False
    
    # Versão dos dados (pegar do primeiro arquivo)
    versao_dados = next(
        (info["versao_dados"] for info in metadata["arquivos"].values()),
        datetime.now().strftime("%Y-%m-%d")
    )
    
    id_versao = f"{DATA_REFERENCIA}_v{versao_dados.replace('-', '')}"
    
    metadata["versao_dados"] = versao_dados
    metadata["id_versao"] = id_versao
    metadata["status"] = "sucesso"
    
    logger.info(f"\n🔖 Versão: {versao_dados}")
    logger.info(f"🆔 ID: {id_versao}")
    
    salvar_metadados(metadata)
    
    logger.info("\n" + "=" * 80)
    logger.info("✅ RAW LAYER CONCLUÍDA")
    logger.info(f"   Referência: {DATA_REFERENCIA}")
    logger.info(f"   Versão: {versao_dados}")
    logger.info("=" * 80)
    
    return True


if __name__ == "__main__":
    exit(0 if executar_camada_raw() else 1)