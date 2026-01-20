"""
MLflow Configuration for ML Experiment Tracking
"""
import os
import mlflow
from mlflow.tracking import MlflowClient

# MLflow Server Settings
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
MLFLOW_EXPERIMENT_NAME = "ecommerce-ml-models"

# Model Registry Settings
MLFLOW_ARTIFACT_LOCATION = "./mlruns"


def setup_mlflow(experiment_name=MLFLOW_EXPERIMENT_NAME):
    """
    Setup MLflow tracking URI and experiment
    
    Args:
        experiment_name (str): Name of the MLflow experiment
        
    Returns:
        str: Experiment ID
    """
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
        experiment_id = experiment.experiment_id
        print(f"Using existing experiment: {experiment_name} (ID: {experiment_id})")
    
    # Set active experiment
    mlflow.set_experiment(experiment_name)
    
    print(f"   MLflow UI: {MLFLOW_TRACKING_URI}")
    
    return experiment_id


def get_mlflow_client():
    """
    Get MLflow client
    
    Returns:
        MlflowClient: MLflow client instance
    """
    return MlflowClient(tracking_uri=MLFLOW_TRACKING_URI)


def log_model_metrics(run_name, model_name, metrics, params=None, model=None):
    """
    Log model metrics, parameters and model to MLflow
    
    Args:
        run_name (str): Name of the MLflow run
        model_name (str): Name of the model
        metrics (dict): Dictionary of metrics to log
        params (dict): Dictionary of parameters to log
        model: Model object to log (optional)
    """
    with mlflow.start_run(run_name=run_name):
        # Log parameters
        if params:
            for key, value in params.items():
                mlflow.log_param(key, value)
        
        # Log metrics
        for key, value in metrics.items():
            mlflow.log_metric(key, value)
        
        # Log model
        if model is not None:
            if hasattr(model, 'save'):  # PySpark model
                mlflow.spark.log_model(model, model_name)
            else:  # Scikit-learn model
                mlflow.sklearn.log_model(model, model_name)
        
        run_id = mlflow.active_run().info.run_id
        print(f"Logged model: {model_name} (Run ID: {run_id})")


def get_best_run(experiment_name=MLFLOW_EXPERIMENT_NAME, metric="rmse", ascending=True):
    """
    Get the best run from an experiment based on a metric
    
    Args:
        experiment_name (str): Name of the experiment
        metric (str): Metric to optimize
        ascending (bool): If True, lower is better; if False, higher is better
        
    Returns:
        Run: Best run object
    """
    client = get_mlflow_client()
    experiment = client.get_experiment_by_name(experiment_name)
    
    if experiment is None:
        print(f"Experiment '{experiment_name}' not found")
        return None
    
    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        order_by=[f"metrics.{metric} {'ASC' if ascending else 'DESC'}"],
        max_results=1
    )
    
    if runs:
        best_run = runs[0]
        print(f"Best run: {best_run.info.run_id}")
        print(f"  - Metric ({metric}): {best_run.data.metrics.get(metric)}")
        return best_run
    else:
        print(f"  - No runs found in experiment '{experiment_name}'")
        return None


if __name__ == "__main__":
    # Test MLflow setup
    setup_mlflow()
    print(f"MLflow configured successfully!")
    print(f"Access UI at: {MLFLOW_TRACKING_URI}")