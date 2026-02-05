#!/usr/bin/env python3
"""
BigData E-commerce Project - Automated Setup

This script automates:
1. Docker container startup (MinIO + MLflow)
2. MinIO and MLflow service configuration
3. Spark JAR dependencies download
4. Dataset generation (SAMPLE and FULL)
5. Dataset upload to MinIO
"""

import subprocess
import sys
import time
from pathlib import Path


class Colors:
    """ANSI colors for terminal output"""
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'


def print_header(message):
    """Print colored header"""
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'='*70}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{message:^70}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{'='*70}{Colors.ENDC}\n")


def print_step(step_num, total_steps, message):
    """Print current step"""
    print(f"{Colors.OKCYAN}[{step_num}/{total_steps}]{Colors.ENDC} {Colors.BOLD}{message}{Colors.ENDC}")


def print_success(message):
    """Print success message"""
    print(f"{Colors.OKGREEN}OK {message}{Colors.ENDC}")


def print_error(message):
    """Print error message"""
    print(f"{Colors.FAIL}ERROR {message}{Colors.ENDC}")


def print_warning(message):
    """Print warning message"""
    print(f"{Colors.WARNING}WARNING {message}{Colors.ENDC}")


def check_docker():
    """
    Verify Docker is installed and running
    
    Returns:
        bool: True if Docker is available, False otherwise
    """
    print_step(1, 5, "Docker Verification")
    
    # Check docker command
    try:
        result = subprocess.run(
            ['docker', '--version'],
            capture_output=True,
            text=True,
            check=True
        )
        print_success(f"Docker installed: {result.stdout.strip()}")
    except (subprocess.CalledProcessError, FileNotFoundError):
        print_error("Docker not found. Install Docker Desktop and retry.")
        return False
    
    # Check Docker daemon
    try:
        result = subprocess.run(
            ['docker', 'ps'],
            capture_output=True,
            text=True,
            check=True
        )
        print_success("Docker daemon running")
        return True
    except subprocess.CalledProcessError:
        print_error("Docker daemon not running. Start Docker Desktop and retry.")
        return False


def start_docker_compose():
    """
    Start Docker containers using docker-compose.yml
    
    Returns:
        bool: True if startup successful, False otherwise
    """
    print_step(2, 5, "Starting Docker Containers (MinIO + MLflow)")
    
    # Verify docker-compose.yml exists
    compose_file = Path('docker-compose.yml')
    if not compose_file.exists():
        print_error("File docker-compose.yml not found in current directory")
        print_warning("Make sure to run the script from project root")
        return False
    
    # Start containers
    try:
        print("   Starting containers in detached mode...")
        result = subprocess.run(
            ['docker', 'compose', 'up', '-d'],
            capture_output=True,
            text=True,
            check=True
        )
        
        # Wait for services to start
        print("   Waiting for services to start (15 seconds)...")
        for i in range(15, 0, -1):
            print(f"   {i}...", end='\r')
            time.sleep(1)
        print("   " + " "*20)  # Clear countdown
        
        # Verify containers running
        result = subprocess.run(
            ['docker', 'compose', 'ps'],
            capture_output=True,
            text=True,
            check=True
        )
        
        print_success("Containers started successfully")
        print(f"\n{result.stdout}")
        return True
        
    except subprocess.CalledProcessError as e:
        print_error(f"Error starting containers: {e}")
        print_warning(f"Output: {e.stderr}")
        return False


def configure_services():
    """
    Configure MinIO and MLflow by executing configuration scripts
    
    Returns:
        bool: True if configuration successful, False otherwise
    """
    print_step(3, 5, "Service Configuration (MinIO + MLflow)")
    
    # Configure MinIO
    print("   Configuring MinIO...")
    minio_config = Path('config/minio_config.py')
    if not minio_config.exists():
        print_error("File config/minio_config.py not found")
        return False
    
    try:
        result = subprocess.run(
            [sys.executable, str(minio_config)],
            capture_output=True,
            text=True,
            check=True
        )
        print_success("MinIO configured")
        
    except subprocess.CalledProcessError as e:
        print_error(f"MinIO configuration error: {e}")
        print_warning(f"Output: {e.stderr}")
        return False
    
    # Configure MLflow
    print("   Configuring MLflow...")
    mlflow_config = Path('config/mlflow_config.py')
    if not mlflow_config.exists():
        print_error("File config/mlflow_config.py not found")
        return False
    
    try:
        result = subprocess.run(
            [sys.executable, str(mlflow_config)],
            capture_output=True,
            text=True,
            check=True
        )
        print_success("MLflow configured")
        return True
        
    except subprocess.CalledProcessError as e:
        print_error(f"MLflow configuration error: {e}")
        print_warning(f"Output: {e.stderr}")
        return False


def download_jars():
    """
    Download required Spark JAR dependencies for Delta Lake and MinIO integration
    
    This downloads:
    - hadoop-aws (for S3/MinIO connectivity)
    - aws-java-sdk-bundle (AWS SDK dependencies)
    - delta-core (Delta Lake core)
    - delta-storage (Delta Lake storage)
    
    Returns:
        bool: True if download successful, False otherwise
    """
    print_step(4, 5, "Downloading Spark JAR Dependencies")
    
    download_script = Path('config/download_jars.py')
    if not download_script.exists():
        print_error("File scripts/download_jars.py not found")
        return False
    
    try:
        print("   Downloading JARs from Maven Central...")
        print("   - hadoop-aws-3.3.1.jar")
        print("   - aws-java-sdk-bundle-1.11.901.jar")
        print("   - delta-core_2.12-2.4.0.jar")
        print("   - delta-storage-2.4.0.jar")
        print()
        
        result = subprocess.run(
            [sys.executable, str(download_script)],
            capture_output=False,  # Show real-time output
            text=True,
            check=True
        )
        
        print_success("Spark JARs downloaded successfully")
        return True
        
    except subprocess.CalledProcessError as e:
        print_error(f"JAR download error: {e}")
        print_warning("You can manually download JARs later if needed")
        print_warning("Notebooks will attempt auto-download on first Spark session")
        return False  # Non-critical error, continue setup


def generate_and_upload_dataset():
    """
    Generate BOTH datasets (sample + full) and upload FULL to MinIO
    
    - SAMPLE: Generated locally for quick testing (not uploaded)
    - FULL: Generated and uploaded to MinIO for analysis
    
    Returns:
        bool: True if operation successful, False otherwise
    """
    print_step(5, 5, "Dataset Generation & Upload")
    
    print("Strategy:")
    print("   - Generate BOTH datasets (sample + full)")
    print("   - SAMPLE stays local for quick testing")
    print("   - FULL uploaded to MinIO for analysis")
    print("   - Total time: ~20-25 minutes")
    
    # Generate BOTH datasets (no user choice)
    print("\n   Generating datasets (SAMPLE + FULL)...")
    dataset_script = Path('scripts/generate_dataset.py')
    if not dataset_script.exists():
        print_error("File scripts/generate_dataset.py not found")
        return False
    
    try:
        # Generate BOTH: send '3' as input for choice
        result = subprocess.run(
            [sys.executable, str(dataset_script)],
            input='3\n',  # Choice 3 = BOTH
            capture_output=False,
            text=True,
            check=True
        )
        print_success("Datasets generated (SAMPLE + FULL)")
        
    except subprocess.CalledProcessError as e:
        print_error(f"Error generating datasets: {e}")
        return False
    
    # Upload ONLY FULL to MinIO
    print("\n   Uploading FULL dataset to MinIO...")
    upload_script = Path('scripts/upload_to_minio.py')
    if not upload_script.exists():
        print_error("File scripts/upload_to_minio.py not found")
        return False
    
    try:
        result = subprocess.run(
            [sys.executable, str(upload_script)],
            capture_output=False,
            text=True,
            check=True
        )
        print_success("FULL dataset uploaded to MinIO")
        print_success("SAMPLE dataset ready locally")
        return True
        
    except subprocess.CalledProcessError as e:
        print_error(f"Error uploading dataset: {e}")
        return False


def main():
    """Main execution function"""
    print_header("BigData E-commerce Project - Automated Setup")
    
    # Step 1: Check Docker
    if not check_docker():
        print_error("\nSetup aborted: Docker not available")
        sys.exit(1)
    
    # Step 2: Start Docker Compose
    if not start_docker_compose():
        print_error("\nSetup aborted: Container startup error")
        sys.exit(1)
    
    # Step 3: Configure services
    if not configure_services():
        print_error("\nSetup aborted: Service configuration error")
        sys.exit(1)
    
    # Step 4: Download Spark JARs
    if not download_jars():
        print_warning("\nJAR download failed, but continuing setup...")
        print_warning("Spark will attempt to download JARs automatically on first run")
    
    # Step 5: Generate and upload dataset
    if not generate_and_upload_dataset():
        print_error("\nSetup aborted: Dataset generation/upload error")
        sys.exit(1)
    
    # Success
    print_header("Setup Completed Successfully!")
    
    print(f"{Colors.OKGREEN}{Colors.BOLD}Available Services:{Colors.ENDC}\n")
    print(f"  MinIO Console:  {Colors.OKBLUE}http://localhost:9001{Colors.ENDC}")
    print(f"    Credentials:    minioadmin / minioadmin")
    print(f"\n  MLflow UI:      {Colors.OKBLUE}http://localhost:5000{Colors.ENDC}")
    print(f"\n  Jupyter:        {Colors.OKBLUE}jupyter notebook{Colors.ENDC}")
    
    print(f"\n{Colors.OKGREEN}{Colors.BOLD}Next Steps:{Colors.ENDC}\n")
    print("  1. Open notebooks in notebooks/")
    print("  2. Explore datasets in MinIO Console")
    print("  3. Verify JARs in config/jars/ directory")
    print("  4. Start working!\n")
    
    print(f"{Colors.OKCYAN}JAR Location:{Colors.ENDC}")
    jars_dir = Path('config/jars')
    if jars_dir.exists():
        jars = list(jars_dir.glob('*.jar'))
        if jars:
            print(f"  {len(jars)} JARs downloaded in config/jars/")
            for jar in sorted(jars):
                size_mb = jar.stat().st_size / (1024*1024)
                print(f"    - {jar.name} ({size_mb:.1f} MB)")
        else:
            print(f"  No JARs found (will auto-download on Spark startup)")
    else:
        print(f"  JAR directory not created (will auto-download on Spark startup)")
    print()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n\n{Colors.WARNING}Setup interrupted by user{Colors.ENDC}")
        sys.exit(1)
    except Exception as e:
        print_error(f"\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
