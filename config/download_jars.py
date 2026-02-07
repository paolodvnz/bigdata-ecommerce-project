#!/usr/bin/env python3
"""
Download Required JARs for Spark + Delta Lake + AVRO + MinIO Integration
"""
import urllib.request
import sys
from pathlib import Path

# Maven Central base URL
MAVEN_BASE = "https://repo1.maven.org/maven2"

# JAR versions (compatible with Spark 3.4.x and Delta Lake 2.4.0)
HADOOP_VERSION = "3.3.1"
AWS_SDK_VERSION = "1.11.901"
DELTA_VERSION = "2.4.0"
SCALA_VERSION = "2.12"
AVRO_VERSION = "3.4.1"

# Define JARs to download
JARS = {
    f"hadoop-aws-{HADOOP_VERSION}.jar": 
        f"{MAVEN_BASE}/org/apache/hadoop/hadoop-aws/{HADOOP_VERSION}/hadoop-aws-{HADOOP_VERSION}.jar",
    
    f"aws-java-sdk-bundle-{AWS_SDK_VERSION}.jar": 
        f"{MAVEN_BASE}/com/amazonaws/aws-java-sdk-bundle/{AWS_SDK_VERSION}/aws-java-sdk-bundle-{AWS_SDK_VERSION}.jar",
    
    f"delta-core_{SCALA_VERSION}-{DELTA_VERSION}.jar": 
        f"{MAVEN_BASE}/io/delta/delta-core_{SCALA_VERSION}/{DELTA_VERSION}/delta-core_{SCALA_VERSION}-{DELTA_VERSION}.jar",
    
    f"delta-storage-{DELTA_VERSION}.jar": 
        f"{MAVEN_BASE}/io/delta/delta-storage/{DELTA_VERSION}/delta-storage-{DELTA_VERSION}.jar",
    
    f"spark-avro-{SCALA_VERSION}-{AVRO_VERSION}.jar": 
        f"{MAVEN_BASE}/org/apache/spark/spark-avro_{SCALA_VERSION}/{AVRO_VERSION}/spark-avro_{SCALA_VERSION}-{AVRO_VERSION}.jar"
}


def download_jar(jar_name, url, jars_dir):
    """Download a single JAR file"""
    output_path = jars_dir / jar_name
    
    if output_path.exists():
        print(f"  {jar_name} already exists (skipping)")
        return True
    
    print(f"  Downloading {jar_name}...")
    print(f"  URL: {url}")
    
    try:
        # Download with progress
        urllib.request.urlretrieve(url, output_path)
        
        # Verify file size
        size_mb = output_path.stat().st_size / (1024 * 1024)
        print(f"    Downloaded successfully ({size_mb:.2f} MB)")
        return True
        
    except Exception as e:
        print(f"    Failed to download: {e}")
        if output_path.exists():
            output_path.unlink()  # Remove partial download
        return False


def main():
    """Main function"""
    
    print("=" * 70)
    print("  Downloading Spark JARs for Delta Lake + AVRO + MinIO Integration")
    print("=" * 70)
    print()
    
    # Determine JAR directory
    script_dir = Path(__file__).parent
    
    # Try to find config/jars directory
    if (script_dir.parent / "config").exists():
        jars_dir = script_dir.parent / "config" / "jars"
    else:
        jars_dir = script_dir / "jars"
    
    # Create directory
    jars_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"JAR directory: {jars_dir}")
    print()
    
    # Download all JARs
    success_count = 0
    failed_jars = []
    
    for jar_name, url in JARS.items():
        if download_jar(jar_name, url, jars_dir):
            success_count += 1
        else:
            failed_jars.append(jar_name)
        print()
    
    # Summary
    print("=" * 70)
    if success_count == len(JARS):
        print("  All JARs downloaded successfully!")
    else:
        print(f"  {success_count}/{len(JARS)} JARs downloaded")
        if failed_jars:
            print("\nFailed downloads:")
            for jar in failed_jars:
                print(f"  - {jar}")
    print("=" * 70)
    print()
    
    # List downloaded files
    print("Downloaded JARs:")
    for jar_file in sorted(jars_dir.glob("*.jar")):
        size_mb = jar_file.stat().st_size / (1024 * 1024)
        print(f"  - {jar_file.name} ({size_mb:.2f} MB)")
    print()
    
    return 0 if success_count == len(JARS) else 1


if __name__ == "__main__":
    sys.exit(main())