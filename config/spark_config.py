"""
Spark Configuration with MinIO Support (Using Local JARs)
Optimized for local mode on 16GB RAM systems.
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
    Get paths to all required JAR files.

    Returns:
        tuple: (comma-separated jar paths string, number of jars found)

    Raises:
        FileNotFoundError: If JARs directory or files are missing.
    """
    if not JARS_DIR.exists():
        raise FileNotFoundError(
            f"JARs directory not found: {JARS_DIR}\n"
            f"Run: python config/download_jars.py"
        )

    jar_files = list(JARS_DIR.glob("*.jar"))

    if not jar_files:
        raise FileNotFoundError(
            f"No JAR files found in {JARS_DIR}\n"
            f"Run: python config/download_jars.py"
        )

    jar_paths = ",".join(str(jar.absolute()) for jar in jar_files)
    return jar_paths, len(jar_files)


def get_spark_session(app_name="BigData-Ecommerce", enable_delta=True):
    """
    Create and configure Spark Session with MinIO and Delta Lake support.

    In local mode the driver IS the executor, so we only configure
    driver memory.  Executor memory settings are ignored by Spark
    but kept minimal to avoid confusion.

    Args:
        app_name: Spark application name.
        enable_delta: Whether to enable Delta Lake extensions.

    Returns:
        SparkSession: Fully configured session.
    """

    # Get JAR paths
    try:
        jar_paths, num_jars = get_jar_paths()
        print(f"Found {num_jars} JAR files")
    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        print("\nTo fix this:")
        print("  python config/download_jars.py")
        raise

    # -----------------------------------------------------------------
    # JVM options -- consolidated into a SINGLE string per role
    # to avoid the second .config() call silently overwriting the first.
    # -----------------------------------------------------------------
    _driver_java_opts = (
        "-Xss16m "
        "-XX:+UseG1GC "
        "-XX:InitiatingHeapOccupancyPercent=35 "
        "-XX:ConcGCThreads=4 "
        "-XX:+ParallelRefProcEnabled "
        "-Dlog4j.logger.org.apache.spark=ERROR "
        "-Dlog4j.logger.org.apache.hadoop=ERROR "
        "-Dlog4j.logger.org.eclipse.jetty=ERROR"
    )

    builder = (
        SparkSession.builder
        .appName(app_name)

        # Memory (local mode: only driver matters)
        .config("spark.driver.memory", "10g")
        .config("spark.driver.maxResultSize", "2g")

        # JVM options
        .config("spark.driver.extraJavaOptions", _driver_java_opts)

        # Delta Lake
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.retentionDurationCheck.enabled",
                "false")

        # JARs
        .config("spark.jars", jar_paths)

        # MinIO / S3A
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # s3:// alias (used by MLflow artifact store)
        .config("spark.hadoop.fs.s3.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3.path.style.access", "true")
        .config("spark.hadoop.fs.s3.connection.ssl.enabled", "false")

        # Parallelism & Shuffle
        .config("spark.default.parallelism", "12")
        .config("spark.sql.shuffle.partitions", "24")

        # Memory management 
        .config("spark.memory.fraction", "0.6")
        .config("spark.memory.storageFraction", "0.4")

        # Adaptive Query Execution
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

        # Parquet
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")

        # UI
        .config("spark.ui.showConsoleProgress", "false")
    )

    # Create Spark session
    if enable_delta:
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    else:
        spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    logging.getLogger("org").setLevel(logging.ERROR)
    logging.getLogger("py4j").setLevel(logging.ERROR)
    
    print(f"Spark Session created: {app_name}")
    print(f"Spark Version: {spark.version}")
    print(f"Spark UI: http://localhost:4040")
    print(f"Delta Lake + MinIO: Enabled")

    return spark


if __name__ == "__main__":
    spark = get_spark_session("Test-Session")
    print("\nConfiguration:")
    for key, value in spark.sparkContext.getConf().getAll():
        if "s3a" in key or "delta" in key or "jars" in key or "memory" in key:
            print(f"  {key} = {value}")
    spark.stop()
    print("Spark Session stopped")