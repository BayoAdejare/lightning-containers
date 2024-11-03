from datetime import datetime, timedelta
from typing import List, Optional, Tuple, Dict, Any
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from pathlib import Path
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import logging
import os
from pydantic import BaseModel, validator

class PipelineConfig(BaseModel):
    """Configuration with validation"""
    BASE_DIR: Path = Path(__file__).parent.resolve()
    DATA_DIR: Path = BASE_DIR / "data"
    LOAD_DIR: Path = DATA_DIR / "Load"
    PROCESSED_DIR: Path = DATA_DIR / "Processed"
    ANALYTICS_DIR: Path = DATA_DIR / "Analytics"
    
    DEFAULT_RETRY_DELAYS: List[int] = [30, 60, 120]
    DEFAULT_CACHE_TTL: str = "1h"
    DEFAULT_HOURS: List[str] = [str(x).zfill(2) for x in range(24)]

    @validator("*")
    def validate_directories(cls, v: Path) -> Path:
        if isinstance(v, Path):
            v.mkdir(parents=True, exist_ok=True)
        return v

    class Config:
        arbitrary_types_allowed = True

config = PipelineConfig()

# Analytics Tasks
@task(retries=2)
def preprocessor(data: pd.DataFrame) -> pd.DataFrame:
    """Preprocess data for clustering"""
    logger = get_run_logger()
    try:
        processed_data = data.copy()
        # Add your preprocessing steps here
        processed_data = processed_data.fillna(0)
        logger.info(f"Preprocessing completed. Shape: {processed_data.shape}")
        return processed_data
    except Exception as e:
        logger.error(f"Preprocessing failed: {str(e)}")
        raise

@task(retries=2)
def kmeans_cluster(data: pd.DataFrame, n_clusters: int = 3) -> Dict[str, Any]:
    """Perform K-means clustering"""
    logger = get_run_logger()
    try:
        kmeans = KMeans(n_clusters=n_clusters, random_state=42)
        clusters = kmeans.fit_predict(data)
        logger.info(f"Clustering completed. Found {n_clusters} clusters")
        return {
            "data": data,
            "clusters": clusters,
            "model": kmeans
        }
    except Exception as e:
        logger.error(f"Clustering failed: {str(e)}")
        raise

@task(retries=2)
def silhouette_evaluator(cluster_results: Dict[str, Any]) -> float:
    """Evaluate clustering using silhouette score"""
    logger = get_run_logger()
    try:
        score = silhouette_score(
            cluster_results["data"], 
            cluster_results["clusters"]
        )
        logger.info(f"Silhouette score: {score}")
        return score
    except Exception as e:
        logger.error(f"Silhouette evaluation failed: {str(e)}")
        raise

@task(retries=2)
def elbow_evaluator(cluster_results: Dict[str, Any]) -> Dict[str, float]:
    """Evaluate clustering using elbow method"""
    logger = get_run_logger()
    try:
        inertias = []
        for k in range(1, 11):
            kmeans = KMeans(n_clusters=k, random_state=42)
            kmeans.fit(cluster_results["data"])
            inertias.append(kmeans.inertia_)
        result = {
            "inertias": inertias,
            "optimal_k": np.argmin(np.diff(inertias)) + 1
        }
        logger.info(f"Elbow analysis completed. Optimal k: {result['optimal_k']}")
        return result
    except Exception as e:
        logger.error(f"Elbow evaluation failed: {str(e)}")
        raise

@task(retries=3, 
      retry_delay_seconds=30, 
      cache_key_fn=task_input_hash,
      cache_expiration=timedelta(hours=1))
def validate_dates(start_date: str, end_date: str) -> Tuple[datetime, datetime]:
    """Validate and parse input dates"""
    logger = get_run_logger()
    try:
        start = datetime.strptime(start_date, "%d/%m/%Y")
        end = datetime.strptime(end_date, "%d/%m/%Y")
        if end < start:
            raise ValueError("End date must be after start date")
        logger.info(f"Date range validated: {start_date} to {end_date}")
        return start, end
    except ValueError as e:
        logger.error(f"Date validation failed: {str(e)}")
        raise

@task
def prepare_hours(hours: Optional[List[str]] = None) -> List[str]:
    """Prepare and validate hours list"""
    if not hours:
        return config.DEFAULT_HOURS
    validated_hours = []
    for hour in hours:
        try:
            h = int(hour)
            if not (0 <= h <= 23):
                raise ValueError(f"Hour {hour} must be between 00-23")
            validated_hours.append(str(h).zfill(2))
        except ValueError:
            raise ValueError(f"Invalid hour format: {hour}")
    return validated_hours

@flow(name="ETL Pipeline", 
      retries=3, 
      retry_delay_seconds=30)
def etl_ingest(
    start_date: str,
    end_date: str,
    hours: Optional[List[str]] = None
) -> bool:
    """Main ETL ingestion flow"""
    logger = get_run_logger()
    logger.info(f"Starting ETL ingestion process")
    
    try:
        # Validate inputs
        start, end = validate_dates(start_date, end_date)
        validated_hours = prepare_hours(hours)
        
        # Create example data for testing
        data = pd.DataFrame({
            'timestamp': pd.date_range(start=start, end=end, freq='H'),
            'value': np.random.randn(24),
            'category': np.random.choice(['A', 'B', 'C'], 24)
        })
        
        # Save to processed directory
        output_path = config.PROCESSED_DIR / "processed_data.parquet"
        data.to_parquet(output_path)
        logger.info(f"Data saved to {output_path}")
        
        return True
        
    except Exception as e:
        logger.error(f"ETL ingestion failed: {str(e)}")
        raise

@flow(name="Clustering Analysis",
      retries=3,
      retry_delay_seconds=30)
def cluster_analysis() -> Dict[str, Any]:
    """Clustering analysis flow"""
    logger = get_run_logger()
    logger.info("Starting clustering analysis")
    
    try:
        # Load data
        data_path = config.PROCESSED_DIR / "processed_data.parquet"
        if not data_path.exists():
            raise FileNotFoundError(f"Processed data not found at {data_path}")
            
        raw_data = pd.read_parquet(data_path)
        
        # Process and cluster
        processed_data = preprocessor(raw_data)
        cluster_results = kmeans_cluster(processed_data)
        
        # Evaluate
        results = {}
        try:
            results["silhouette_score"] = silhouette_evaluator(cluster_results)
        except Exception as e:
            logger.warning(f"Silhouette analysis failed: {str(e)}")
            results["elbow_analysis"] = elbow_evaluator(cluster_results)
        
        # Save results
        pd.DataFrame({
            "cluster": cluster_results["clusters"],
            **{f"feature_{i}": processed_data.iloc[:, i] 
               for i in range(processed_data.shape[1])}
        }).to_parquet(config.ANALYTICS_DIR / "clustering_results.parquet")
        
        return results
        
    except Exception as e:
        logger.error(f"Clustering analysis failed: {str(e)}")
        raise

@flow(name="Dashboard Refresh",
      retries=2,
      retry_delay_seconds=5)
def dashboard_refresh() -> str:
    """Dashboard refresh flow"""
    logger = get_run_logger()
    logger.info("Starting dashboard refresh")
    return "Dashboard Flow completed successfully!"

@flow(name="Main Pipeline")
def main() -> None:
    """Main entry point for the application"""
    logger = get_run_logger()
    
    try:
        # Run pipeline steps
        etl_success = etl_ingest(
            start_date="09/10/2024",
            end_date="09/10/2024",
            hours=["00", "01", "02"]
        )
        
        if etl_success:
            clustering_results = cluster_analysis()
            logger.info(f"Clustering completed with results: {clustering_results}")
            dashboard_refresh()
            
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
