# BigData E-commerce Analytics

Progetto di analisi Big Data su dataset e-commerce con **100M+ transazioni**, utilizzando **PySpark**, **Dask**, **Delta Lake** e **MLflow**.

## Panoramica

Applicazione enterprise-level di tecnologie Big Data per l'analisi di un dataset e-commerce sintetico:
- **Dataset**: 1M customers, 50K products, 100M transactions
- **Storage**: MinIO (S3-compatible)
- **Processing**: Pandas, Dask, PySpark
- **ML Tracking**: MLflow
- **Data Lake**: Delta Lake
- **Streaming**: Spark Structured Streaming

## Quick Start

### Prerequisiti

- **Docker Desktop** installato e in esecuzione
- **Python 3.10+** con conda/venv

### Setup Automatico (Raccomandato)

Il progetto include uno script di setup automatico che configura tutto:

```bash
# 1. Clone repository
git clone https://github.com/paolodvnz/bigdata-ecommerce-project.git
cd bigdata-ecommerce-project

# 2. Crea ambiente virtuale
conda create -n bigdata python=3.10 -y
conda activate bigdata

# 3. Installa dipendenze
pip install -r requirements.txt

# 4. Esegui setup automatico
python setup.py
```

Lo script **setup.py** esegue automaticamente:
1. Verifica Docker installato e running
2. Avvia container MinIO + MLflow
3. Configura servizi e crea bucket
4. Download JARs 
5. Genera dataset (SAMPLE o FULL)
6. Upload dataset su MinIO


### Setup Manuale (Avanzato)

Se preferisci configurare manualmente:

```bash
# 1. Avvia container Docker
docker compose up -d

# 2. Configura MinIO
python config/minio_config.py

# 3. Configura MLflow
python config/mlflow_config.py

# 4. Download JARs
python config/download_jars.py

# 5. Genera dataset
python scripts/generate_dataset.py --mode both    # oppure full o sample

# 6. Upload su MinIO
python scripts/upload_to_minio.py                 # upload solo full
```

**Scelta Dataset:**
- **SAMPLE**: 20K customers, 1K products, 20 transactions (~3-5 minuto)
- **FULL**: 1M customers, 50K products, 100M transactions (~10-15 minuti)
- **ENTRAMBI**: Prima SAMPLE poi FULL

### Verifica Installazione

Dopo il setup, verifica che i servizi siano operativi:

- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin)
- **MLflow UI**: http://localhost:5000
- **Spark UI**: http://localhost:4040 (quando Spark session attiva)

## Struttura Progetto

```
bigdata-ecommerce-project/
│
├── config/                        # Configurazioni servizi
│   ├── download_jars.py             # Download jars
│   ├── minio_config.py              # MinIO S3-compatible
│   ├── spark_config.py              # SparkSession setup
│   └── mlflow_config.py             # ML experiment tracking 
│
├── scripts/                       # Script generazione e upload dati
│   ├── generate_dataset.py          # Genera dataset sintetici
│   ├── upload_to_minio.py           # Upload su MinIO
│   └── utils/                       # Helper functions
│
├── notebooks/                     # 5 Jupyter notebooks
│   ├── 01_pandas_limits.ipynb
│   ├── 02_dask_distributed.ipynb
│   ├── 03_pyspark.ipynb
│   ├── 04_pyspark_ml.ipynb
│   └── 05_pyspark_streaming.ipynb
│
├── data/
│   ├── sample/                    # Dataset ridotto
│   └── raw/                       # Dataset completo
│
├── docker-compose.yml             # MinIO + MLflow
├── setup.py                       # Setup automatico
└── requirements.txt               # Dipendenze Python
```

## Notebook

Il progetto è organizzato in 5 notebook Jupyter che coprono gli aspetti fondamentali del Big Data processing:

1. **Pandas Limits** - Analisi limiti Pandas
2. **Dask Distributed** - Calcolo distribuito con Dask
3. **PySpark ETL** - Calcolo distribuito con PySpark e Pipeline ETL
4. **PySpark ML** - Training ML con PySpark e MLflow Tracking
5. **PySpark Streaming** - Streaming e Medallion Architecture con PySpark e Delta-Lake

## Utilizzo

### Avvio Jupyter Notebook

```bash
# Attiva ambiente
conda activate bigdata

# Avvia Jupyter
jupyter notebook

# Naviga in notebooks/ e apri i notebook
```

### Accesso Dati nei Notebook

```python
# Setup
from config.spark_config import get_spark_session
from config.minio_config import get_s3a_path

# Spark session
spark = get_spark_session("MyNotebook")

# Carica dati da MinIO
customers = spark.read.parquet(get_s3a_path("raw/", "customers.parquet"))
products = spark.read.parquet(get_s3a_path("raw/", "products.parquet"))
transactions = spark.read.parquet(get_s3a_path("raw/", "transactions/"))
```

## Stop e Cleanup

```bash
# Stop container (dati preservati)
docker compose stop

# Stop e rimozione container
docker compose down

# Cleanup completo (attenzione: cancella tutti i dati!)
docker compose down -v
rm -rf minio-data/ mlruns/ data/raw/ data/sample/
```

## Tecnologie

- **Python 3.10**
- **Pandas 2.3.3** - Analisi dati piccoli/medi
- **Dask 2023.5.0** - Calcolo distribuito
- **PySpark 3.4.1** - Big Data processing
- **Delta Lake 2.4.0** - ACID transactions
- **MLflow 2.9.2** - ML experiment tracking
- **MinIO** - S3-compatible storage
- **Docker** - Containerization

## Troubleshooting

### Docker non si avvia
```bash
# Verifica Docker Desktop running
docker ps

# Riavvia Docker Desktop se necessario
```

### Container già esistenti
```bash
# Rimuovi container esistenti
docker compose down
docker compose up -d
```

### MinIO non accessibile
- Verifica container running: `docker compose ps`
- Controlla porta 9001 libera: `lsof -i :9001`
- Accedi a http://localhost:9001 (minioadmin/minioadmin)

### MLflow non accessibile
- Verifica container running: `docker compose ps`
- Controlla porta 5000 libera: `lsof -i :5000`
- Accedi a http://localhost:5000

### Dataset non trovati
```bash
# Rigenera dataset
python scripts/generate_dataset.py --mode sample

# Upload su MinIO
python scripts/upload_to_minio.py --mode sample
```

## Licenza

MIT License - vedi file LICENSE

## Autore

Paolo D'Avanzo - Progetto Finale BigData E-commerce Analytics

---
