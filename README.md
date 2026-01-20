# BigData E-commerce Analytics

Progetto completo di analisi Big Data su dataset e-commerce con **100M+ transazioni**, utilizzando **PySpark**, **Dask**, **Delta Lake** e **MLflow**.

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
- **Git** e GitHub Desktop (opzionale)

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
4. Genera dataset (scelta interattiva: SAMPLE o FULL)
5. Upload dataset su MinIO

**Scelta Dataset:**
- **SAMPLE**: 1K customers, 100 products, 100K transactions (~1 minuto)
- **FULL**: 1M customers, 50K products, 100M transactions (~15-20 minuti)
- **ENTRAMBI**: Prima SAMPLE poi FULL

### Setup Manuale (Avanzato)

Se preferisci configurare manualmente:

```bash
# 1. Avvia container Docker
docker compose up -d

# 2. Configura MinIO
python config/minio_config.py

# 3. Configura MLflow
python config/mlflow_config.py

# 4. Genera dataset
python scripts/generate_dataset.py --mode sample  # oppure full o both

# 5. Upload su MinIO
python scripts/upload_to_minio.py --mode sample   # oppure full o both
```

### Verifica Installazione

Dopo il setup, verifica che i servizi siano operativi:

- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin)
- **MLflow UI**: http://localhost:5000
- **Spark UI**: http://localhost:4040 (quando Spark session attiva)

## Struttura Progetto

```
bigdata-ecommerce-project/
│
├── config/                    # Configurazioni servizi
│   ├── minio_config.py       # MinIO S3-compatible
│   ├── spark_config.py       # SparkSession setup
│   └── mlflow_config.py      # ML experiment tracking
│
├── scripts/                   # Script generazione dati
│   ├── generate_dataset.py   # Genera dataset sintetici
│   ├── upload_to_minio.py    # Upload su MinIO
│   └── utils/                # Helper functions
│
├── notebooks/                 # 8 Jupyter notebooks (deliverable)
│   ├── 01_pandas_dask_limits.ipynb
│   ├── 02_dask_dataframe.ipynb
│   ├── 03_architetture_bigdata.ipynb
│   ├── 04_pyspark_intro.ipynb
│   ├── 05_spark_sql_pipeline.ipynb
│   ├── 06_performance_ml.ipynb
│   ├── 07_streaming_datalake.ipynb
│   └── 08_debugging_monitoring.ipynb
│
├── src/                       # Codice modulare
│   ├── etl/                  # Pipeline ETL
│   └── ml/                   # Modelli ML
│
├── data/
│   ├── sample/               # Dataset ridotto (GitHub)
│   ├── raw/                  # Dataset completo (locale)
│   └── schemas/              # JSON schemas
│
├── docker-compose.yml        # MinIO + MLflow
├── setup.py                  # Setup automatico
└── requirements.txt          # Dipendenze Python
```

## Notebook

Il progetto è organizzato in 8 notebook Jupyter che coprono tutti gli aspetti del Big Data processing:

1. **Pandas/Dask Limits** - Analisi limiti Pandas e introduzione Dask
2. **Dask DataFrame** - Calcolo distribuito con Dask
3. **Architetture BigData** - Storage distribuito (Parquet, ORC, S3)
4. **PySpark Intro** - Introduzione a Spark DataFrame
5. **Spark SQL** - Query distribuite e pipeline ETL
6. **Performance + ML** - Tuning e Machine Learning con MLflow
7. **Streaming + Delta** - Real-time processing e Data Lake
8. **Debugging + Insight** - Monitoring e business analytics

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
customers = spark.read.parquet(get_s3a_path("sample/", "customers.parquet"))
products = spark.read.parquet(get_s3a_path("sample/", "products.parquet"))
transactions = spark.read.parquet(get_s3a_path("sample/", "transactions/"))
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

## Caratteristiche Chiave

### Big Data Processing
- Dataset 100M+ transazioni
- Elaborazione distribuita con Spark
- Ottimizzazioni performance (partitioning, caching, shuffle)

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

## Contributi

Questo è un progetto accademico. Per suggerimenti o miglioramenti, apri una issue.

## Licenza

MIT License - vedi file LICENSE

## Autore

Paolo - Progetto Finale BigData E-commerce Analytics

---

**Pronto per iniziare?** Esegui `python setup.py` e il progetto sarà configurato in pochi minuti!
