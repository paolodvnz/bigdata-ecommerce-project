"""
Spark Configuration with MinIO Support
"""
import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def get_spark_session(app_name="BigData-Ecommerce", enable_delta=True):
    """
    Create and configure Spark Session with MinIO and Delta Lake support
    
    Args:
        app_name (str): Application name
        enable_delta (bool): Enable Delta Lake support
        
    Returns:
        SparkSession: Configured Spark session
    """
    # Base configuration
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", 
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.maxResultSize", "2g")
    
    # Enable Delta Lake if requested
    if enable_delta:
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    else:
        spark = builder.getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"Spark Session created: {app_name}")
    print(f"Spark Version: {spark.version}")
    print(f"Spark UI: http://localhost:4040")
    
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
    print(spark.sparkContext.getConf().getAll())
    stop_spark_session(spark)