"""
MLflow Configuration for ML Experiment Tracking with MinIO Artifact Store
"""

import os
import mlflow
from mlflow.tracking import MlflowClient

# MLflow Server Settings
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
MLFLOW_EXPERIMENT_NAME = "ecommerce-ml-models"

# MinIO S3-Compatible Artifact Store (for ALL models - sklearn AND Spark!)
MLFLOW_ARTIFACT_LOCATION = "s3://bigdata-ecommerce/mlflow-artifacts/"

# MinIO Connection Settings
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")


def setup_s3_client():
    """
    Configure boto3 and Spark for MinIO S3-compatible storage
    """
    os.environ['AWS_ACCESS_KEY_ID'] = MINIO_ACCESS_KEY
    os.environ['AWS_SECRET_ACCESS_KEY'] = MINIO_SECRET_KEY
    os.environ['MLFLOW_S3_ENDPOINT_URL'] = f"http://{MINIO_ENDPOINT}"
    os.environ['MLFLOW_S3_IGNORE_TLS'] = "true"
    
    print(f"S3 client configured for MinIO at {MINIO_ENDPOINT}")


def setup_mlflow(experiment_name=MLFLOW_EXPERIMENT_NAME):
    """
    Setup MLflow tracking URI and experiment with MinIO artifact store
    
    Args:
        experiment_name (str): Name of the MLflow experiment
        
    Returns:
        str: Experiment ID
    """
    # Configure S3 client for MinIO
    setup_s3_client()
    
    # Set tracking URI
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    
    # Create or get experiment
    try:
        experiment_id = mlflow.create_experiment(
            experiment_name,
            artifact_location=MLFLOW_ARTIFACT_LOCATION
        )
        print(f"Created new experiment: {experiment_name} (ID: {experiment_id})")
    except Exception:
        experiment = mlflow.get_experiment_by_name(experiment_name)
        if experiment is None:
            raise ValueError(f"Experiment '{experiment_name}' not found")
        experiment_id = experiment.experiment_id
        print(f"Using existing experiment: {experiment_name} (ID: {experiment_id})")
    
    # Set active experiment
    mlflow.set_experiment(experiment_name)
    
    print(f"MLflow UI: {MLFLOW_TRACKING_URI}")
    print(f"Artifacts Location: {MLFLOW_ARTIFACT_LOCATION}")
    
    return experiment_id


def get_mlflow_client():
    """
    Get MLflow client
    
    Returns:
        MlflowClient: MLflow client instance
    """
    return MlflowClient(tracking_uri=MLFLOW_TRACKING_URI)


def log_model(run_name, model_name, model, metrics, params=None, tags=None, pip_requirements=["pyspark==3.4.1"]):
    """
    Log model (sklearn or Spark) to MLflow with MinIO artifact store
    
    Args:
        run_name (str): Name of the MLflow run
        model_name (str): Name of the model
        model: Spark Model object
        metrics (dict): Dictionary of metrics to log
        params (dict): Dictionary of parameters to log
        tags (dict): Dictionary of tags to set
        spark_model (bool): If True, use mlflow.spark.log_model
        pip_requirements (list): List of pip requirements (e.g., ["pyspark==3.4.1", "numpy==1.26.0"])
        If None, MLflow will try to infer them automatically
        
    Returns:
        str: Run ID
        
    Example:
        >>> log_model(
        ...     run_name="als-v1",
        ...     model_name="als_recommender",
        ...     model=als_model,
        ...     metrics={"rmse": 0.85},
        ...     params={"rank": 10},
                tags={"phase": "development"}
        ...     spark_model=True,
        ...     pip_requirements=["pyspark==3.4.1"]
        ... )
    """
    
    with mlflow.start_run(run_name=run_name):
        # Log parameters
        if params:
            for key, value in params.items():
                mlflow.log_param(key, value)
        
        # Log metrics
        for key, value in metrics.items():
            mlflow.log_metric(key, value)

        # Set Tags
        if tags:
            for key, value in tags.items():
                mlflow.set_tag(key, value)
                
        # Log model (to MinIO s3:// artifact store)
        if model is not None:
            try:
                mlflow.spark.log_model(
                    model, 
                    model_name,
                    pip_requirements=pip_requirements
                )
                print(f"  Spark model logged to MLflow")
                run_id = mlflow.active_run().info.run_id
                print(f"    Run ID: {run_id}")
                print(f"    Artifacts in MinIO: {MLFLOW_ARTIFACT_LOCATION}{run_id}/")
                return run_id
                
            except Exception as e:
                print(f"  Error logging model: {e}")
                raise
        
        return mlflow.active_run().info.run_id


if __name__ == "__main__":
    # Test MLflow setup
    print("="*60)
    print("MLflow Configuration Test")
    print("="*60)
    
    try:
        experiment_id = setup_mlflow()
        print(f"\nMLflow configured successfully!")
        print(f"Experiment ID: {experiment_id}")
        print(f"\nAccess MLflow UI at: {MLFLOW_TRACKING_URI}")
        print(f"\nALL models stored in MinIO:")
        print(f"  {MLFLOW_ARTIFACT_LOCATION}")
    except Exception as e:
        print(f"\nError configuring MLflow: {e}")
        print("\nMake sure:")
        print("1. Docker containers are running: docker compose up -d")
        print("2. MinIO is accessible at localhost:9000")
        print("3. MLflow is accessible at localhost:5000")
