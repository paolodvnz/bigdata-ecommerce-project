"""
Spark Configuration with MinIO Support (Using Local JARs)
"""
import os
from pathlib import Path
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import logging


# JAR directory
JARS_DIR = Path(__file__).parent / "jars"


def get_jar_paths():
    """
    Get paths to all required JAR files
    
    Returns:
        str: Comma-separated paths to JAR files
    """
    if not JARS_DIR.exists():
        raise FileNotFoundError(
            f"JARs directory not found: {JARS_DIR}\n"
            f"Run: bash scripts/download_spark_jars.sh"
        )
    
    jar_files = list(JARS_DIR.glob("*.jar"))
    
    if not jar_files:
        raise FileNotFoundError(
            f"No JAR files found in {JARS_DIR}\n"
            f"Run: bash scripts/download_spark_jars.sh"
        )
    
    # Convert to comma-separated string of absolute paths
    jar_paths = ",".join(str(jar.absolute()) for jar in jar_files)
    
    return jar_paths, len(jar_files)


def get_spark_session(app_name="BigData-Ecommerce", enable_delta=True):
    """
    Create and configure Spark Session with MinIO and Delta Lake support
    Using manually downloaded JARs for reliability
    
    Args:
        app_name (str): Application name
        enable_delta (bool): Enable Delta Lake support
        
    Returns:
        SparkSession: Configured Spark session
    """
    
    logging.getLogger("org").setLevel(logging.ERROR)
    logging.getLogger("py4j").setLevel(logging.ERROR)

    # Get JAR paths
    try:
        jar_paths, num_jars = get_jar_paths()
        print(f"Found {num_jars} JAR files")
    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        print("\nTo fix this:")
        print("  run ../config/download_jars.py")
        raise
    
    # Base configuration - using spark.jars instead of spark.jars.packages
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.extraJavaOptions", 
        "-Dlog4j.logger.org.apache.spark=ERROR "
        "-Dlog4j.logger.org.apache.hadoop=ERROR "
        "-Dlog4j.logger.org.eclipse.jetty=ERROR") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars", jar_paths) \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.maxResultSize", "2g")
    
    # Create Spark session
    if enable_delta:
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    else:
        spark = builder.getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("ERROR")
    
    print(f"Spark Session created: {app_name}")
    print(f"Spark Version: {spark.version}")
    print(f"Spark UI: http://localhost:4040")
    print(f"Delta Lake + MinIO: Enabled")
    
    # Verify S3A FileSystem is available
    try:
        spark.sparkContext._jvm.org.apache.hadoop.fs.s3a.S3AFileSystem
        print("  S3AFileSystem loaded successfully")
    except Exception as e:
        print(f"  WARNING: S3AFileSystem not available: {e}")
    
    return spark


def stop_spark_session(spark):
    """
    Stop Spark session
    
    Args:
        spark (SparkSession): Spark session to stop
    """
    if spark:
        spark.stop()
        print("Spark Session stopped")


# Common Spark configurations for different scenarios
SPARK_CONFIGS = {
    "development": {
        "spark.driver.memory": "2g",
        "spark.executor.memory": "2g",
        "spark.sql.shuffle.partitions": "10"
    },
    "production": {
        "spark.driver.memory": "8g",
        "spark.executor.memory": "8g",
        "spark.sql.shuffle.partitions": "200"
    },
    "streaming": {
        "spark.streaming.stopGracefullyOnShutdown": "true",
        "spark.sql.streaming.schemaInference": "true"
    }
}


if __name__ == "__main__":
    # Test Spark session creation
    spark = get_spark_session("Test-Session")
    print("\nConfiguration:")
    for key, value in spark.sparkContext.getConf().getAll():
        if 's3a' in key or 'delta' in key or 'jars' in key:
            print(f"  {key} = {value}")
    stop_spark_session(spark)