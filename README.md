# 🏢 Pipeline de Dados - CNPJs da Receita Federal

<div align="center">

![Python Version](https://img.shields.io/badge/python-3.10+-blue.svg)
![PySpark](https://img.shields.io/badge/PySpark-3.5-orange.svg)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.0-green.svg)
![Docker](https://img.shields.io/badge/Docker-Compose-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

Pipeline completo de ETL para processamento dos dados públicos de CNPJs da Receita Federal, implementando a Medallion Architecture com PySpark e Delta Lake.

[Funcionalidades](#-funcionalidades) •
[Arquitetura](#-arquitetura) •
[Instalação](#-instalação) •
[Uso](#-uso) •
[Documentação](#-documentação)

</div>

---

## 📋 Sobre o Projeto

Este projeto implementa um **pipeline de dados completo** para ingestão e processamento dos dados públicos de CNPJs da Receita Federal, disponibilizados em [dados.gov.br](https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj).

### 🎯 Objetivo

Criar uma solução robusta e escalável que:

- Download automático dos dados da Receita Federal
- Processamento de milhões de registros de forma eficiente
- Validações e regras de negócio aplicadas
- Qualidade e governança de dados (LGPD)
- Dados tratados disponíveis para consumo analítico e transacional

### 💡 Contexto do Desafio

Desenvolvido como parte de um desafio técnico para avaliar competências em:

- Engenharia de dados e arquitetura de pipelines
- Processamento distribuído com PySpark
- Modelagem de dados e Data Lakehouse
- Containerização e orquestração de serviços
- Boas práticas de desenvolvimento e documentação

---

## ✨ Funcionalidades

### Core Features

- **Download Automático**: Baixa arquivos .zip da Receita Federal com retry e validação
- **Processamento Distribuído**: Utiliza PySpark para processar grandes volumes
- **Medallion Architecture**: Implementação completa (Raw → Bronze → Silver → Gold)
- **Delta Lake**: ACID transactions, time travel e schema evolution
- **SCD Type 2**: Rastreamento histórico de mudanças em empresas
- **Conformidade LGPD**: Mascaramento automático de CPFs
- **Data Quality**: Validações, deduplicação e quarentena de dados
- **OBT (One Big Table)**: Tabela final desnormalizada para análise
- **PostgreSQL**: Persistência para aplicações transacionais
- **Containerização**: Ambiente completo com Docker Compose

### Features Avançadas

- **Performance**: OPTIMIZE e Z-ORDERING no Delta Lake
- **Logging Estruturado**: Rastreabilidade completa do pipeline
- **Auditoria**: Tabelas de quarentena para análise de dados rejeitados
- **FinOps**: Limpeza automática de arquivos intermediários

---

## 🏗️ Arquitetura

![Diagrama do Pipeline de Dados](https://raw.githubusercontent.com/rafaelpds/receita-federal-cnpj/main/docs/pipeline_flow.png)

### Medallion Architecture

**RAW LAYER**
- Arquivos ZIP da Receita Federal
- Download automático via HTTP
- Armazenamento temporário

**BRONZE LAYER**
- Dados brutos em formato Delta Lake
- Schema enforcement
- Particionamento
- Sem transformações

**SILVER LAYER**
- Validações e limpeza de dados
- Deduplicação
- Normalização de campos
- SCD Type 2 para empresas (rastreamento histórico)
- Mascaramento LGPD (CPFs)
- OPTIMIZE com Z-ORDERING

**GOLD LAYER**
- Agregações e regras de negócio
- OBT (One Big Table) desnormalizada
- Apenas registros ativos (SCD Type 2)
- Persistência: Delta Lake + PostgreSQL

### SCD Type 2 - Slowly Changing Dimensions

A camada **Silver** implementa SCD Type 2 para a tabela de **empresas**, permitindo rastrear mudanças históricas:

**Colunas de Controle:**
- `data_inicio`: Quando o registro ficou ativo
- `data_fim`: Quando foi substituído (NULL = ativo)
- `ativo`: True = versão atual, False = histórico
- `hash_chave`: Detecta mudanças nos dados

**Exemplo de Histórico:**

| cnpj | razao_social | data_inicio | data_fim | ativo |
|------|--------------|-------------|----------|-------|
| 00000000 | BANCO DO BRASIL SA | 2025-01-01 10:00 | 2025-01-02 15:00 | False |
| 00000000 | BB LTDA | 2025-01-02 15:00 | NULL | True |

**Por que SCD Type 2?**
- Empresas mudam razão social, capital social, porte
- Importante para auditorias e análises temporais
- Sócios tem snapshot simples (não precisam de histórico)

### Stack Tecnológica

**Processamento:**
- Python 3.10+
- PySpark 3.5.0
- Delta Lake 3.0.0

**Armazenamento:**
- Delta Lake (Data Lakehouse)
- PostgreSQL 15 (OLTP)

**Infraestrutura:**
- Docker & Docker Compose
- Ubuntu/Linux

---

## 🚀 Instalação

### Pré-requisitos

- Docker 20.10+
- Docker Compose 2.0+
- 8GB RAM mínimo
- 20GB espaço em disco

### 1. Clone o Repositório

```bash
git clone https://github.com/rafaelpds/receita-federal-cnpj.git
cd receita-federal-cnpj
```

### 2. Suba os Serviços

```bash
docker-compose up
```

Isso irá:
- Construir a imagem Docker com PySpark e dependências
- Subir PostgreSQL
- Executar o pipeline automaticamente

### 3. Acompanhe os Logs

```bash
docker-compose logs -f cnpj_app
```

---

## 📊 Uso

### Executar Pipeline Completo

```bash
docker-compose up
```

### Executar Camadas Individualmente

```bash
# Apenas Bronze
docker-compose run cnpj_app python -m pipelines.bronze_rf_cnpj

# Apenas Silver
docker-compose run cnpj_app python -m pipelines.silver_rf_cnpj

# Apenas Gold
docker-compose run cnpj_app python -m pipelines.gold_rf_cnpj
```

---

## 📁 Estrutura do Projeto

```
receita-federal-cnpj/
├── data/                          # Dados persistidos (gitignored)
│   ├── bronze/                    # Delta Lake - dados brutos
│   ├── gold/                      # Delta Lake - dados agregados
│   ├── raw/                       # Arquivos ZIP
│   └── silver/                    # Delta Lake - dados limpos
├── docs/
│   └── pipeline_flow.png          # Diagrama da arquitetura
├── jars/                          # Dependências Java (Delta, PostgreSQL)
├── pipelines/
│   ├── __pycache__/
│   ├── __init__.py
│   ├── bronze_rf_cnpj.py          # Ingestão de dados brutos
│   ├── gold_rf_cnpj.py            # Agregações + regras de negócio
│   ├── raw_rf_cnpj.py             # Download dos arquivos
│   └── silver_rf_cnpj.py          # Limpeza + SCD Type 2
├── .gitignore
├── docker-compose.yml             # Orquestração de serviços
├── main.py                        # Orquestrador principal
├── README.md                      # Este arquivo
└── requirements.txt               # Dependências Python
```

---

## 📖 Documentação

### Camadas do Pipeline

#### Bronze Layer
- **Objetivo**: Ingestão de dados brutos sem transformações
- **Formato**: Delta Lake
- **Schema**: Preserva estrutura original dos CSVs
- **Validações**: Apenas schema enforcement

#### Silver Layer
- **Objetivo**: Dados limpos e validados
- **Transformações**:
  - Validação de CNPJs (8 dígitos numéricos)
  - Normalização de textos (UPPER, TRIM)
  - Deduplicação
  - Mascaramento de CPFs (LGPD)
  - SCD Type 2 para empresas
- **Otimização**: OPTIMIZE + Z-ORDERING por CNPJ

#### Gold Layer
- **Objetivo**: Tabela analítica (OBT)
- **Regras de Negócio**:
  - `qtde_socios`: COUNT de sócios por CNPJ
  - `flag_socio_estrangeiro`: True se ≥1 sócio estrangeiro
  - `doc_alvo`: True se cod_porte='03' E qtde_socios > 1
- **Destinos**: Delta Lake + PostgreSQL

### Schema dos Dados

**Empresas (Silver)**
```
cnpj: string
razao_social: string
natureza_juridica: int
qualificacao_responsavel: int
capital_social: float
cod_porte: string
hash_chave: string
data_inicio: timestamp
data_fim: timestamp
ativo: boolean
```

**Sócios (Silver)**
```
cnpj: string
tipo_socio: int
nome_socio: string
documento_socio: string (mascarado)
codigo_qualificacao_socio: string
is_estrangeiro: boolean
```

**Resultado Final (Gold)**
```
cnpj: string
qtde_socios: int
flag_socio_estrangeiro: boolean
doc_alvo: boolean
```

---

## 🔧 Configurações

### Variáveis de Ambiente

Edite o `docker-compose.yml`:

```yaml
environment:
  - POSTGRES_HOST=postgres
  - POSTGRES_PORT=5432
  - POSTGRES_DB=cnpj_db
  - POSTGRES_USER=cnpj_user
  - POSTGRES_PASSWORD=cnpj_pass
```

### Ajustes de Performance

No `silver_rf_cnpj.py`:

```python
.config("spark.driver.memory", "2g")        # Memória do driver
.config("spark.executor.memory", "2g")      # Memória do executor
.config("spark.sql.shuffle.partitions", "4") # Paralelismo
```

---

## 🤝 Contribuindo

Contribuições são bem-vindas! Por favor:

1. Fork o projeto
2. Crie uma branch para sua feature (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'Add some AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request

---

## 📝 Licença

Este projeto está sob a licença MIT. Veja o arquivo `LICENSE` para mais detalhes.

---

## 👤 Autor

**Rafael Santos**

- GitHub: [@rafaelpds](https://github.com/rafaelpds)
- LinkedIn: [Rafael Pereira](https://linkedin.com/in/rafaelpds)


---

<div align="center">

**Desenvolvido com ❤️ para o desafio de Data Engineering**

</div>
