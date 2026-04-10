# 🌦️ Data Engineering: Clima & Crypto Brasil

Pipeline de dados em tempo real coletando dados climáticos de 5 cidades brasileiras e preços de criptomoedas, processados com arquitetura Medallion no Databricks.

## 🏗️ Arquitetura
APIs (Open-Meteo + CoinGecko)
↓
Cloud Run (Python)
↓
Cloud Scheduler (1x/hora)
↓
GCS - Data Lake
raw/clima/ | raw/crypto/
↓
Databricks
Bronze → Silver → Gold
↓
Dashboard ao vivo
## 🛠️ Stack Tecnológica

- **Ingestão**: Python + Flask + Google Cloud Run
- **Agendamento**: Google Cloud Scheduler
- **Storage**: Google Cloud Storage (Data Lake)
- **Processamento**: Databricks + PySpark + Delta Lake
- **Arquitetura**: Medallion (Bronze / Silver / Gold)
- **Dashboard**: Databricks SQL Dashboard

## 📊 Dados Coletados

### Clima (Open-Meteo API - gratuita)
- São Paulo, Rio de Janeiro, Brasília, Manaus, Porto Alegre
- Temperatura, velocidade do vento, sensação térmica

### Criptomoedas (CoinGecko API - gratuita)
- Bitcoin, Ethereum, Solana, Cardano, Ripple
- Preço em BRL e USD, variação 24h, market cap

## 🥉 Camadas do Data Lake

| Camada | Descrição |
|--------|-----------|
| **Bronze** | Dados brutos do GCS convertidos para Delta Table |
| **Silver** | Dados limpos, tipados e enriquecidos com classificações |
| **Gold** | KPIs e análises cruzadas clima + crypto |

## 💰 Custo Mensal

| Serviço | Custo |
|---------|-------|
| Google Cloud Storage | ~R$ 0 (free tier) |
| Cloud Run | ~R$ 0 (free tier) |
| Cloud Scheduler | R$ 0 (3 jobs gratuitos) |
| Databricks | R$ 0 (Free Edition) |
| **Total** | **~R$ 0/mês** |

## 📁 Estrutura do Projeto
├── ingestion/
│   ├── main.py              # Cloud Run - coleta dados das APIs
│   └── requirements.txt
├── notebooks/
│   ├── 01_bronze.py         # Ingestão incremental GCS → Delta
│   ├── 02_silver.py         # Limpeza e enriquecimento
│   └── 03_gold.py           # KPIs e análises
└── README.md
