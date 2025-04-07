from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from typing import List, Optional, Tuple, Dict, Any
from pydantic import BaseModel, field_validator
from pathlib import Path

from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.client.orchestration import get_client
from prefect.deployments import run_deployment

from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

import pandas as pd
import numpy as np
import logging
import asyncio
import os

from tasks import etl, analytics

class PipelineConfig(BaseModel):
    """Configuration with directory validation"""
    BASE_DIR: Path = Path("src")
    DATA_DIR: Path = BASE_DIR / "data"
    RAW_DIR: Path = DATA_DIR / "Raw"
    PROCESSED_DIR: Path = DATA_DIR / "Processed"
    ANALYTICS_DIR: Path = DATA_DIR / "Analytics"
    
    DEFAULT_RETRY_DELAYS: List[int] = [30, 60, 120]
    DEFAULT_CACHE_TTL: str = "1h"
    DEFAULT_HOURS: List[str] = [str(x).zfill(2) for x in range(24)]

    @field_validator('RAW_DIR', 'PROCESSED_DIR', 'ANALYTICS_DIR', mode='after')
    @classmethod
    def validate_data_dirs(cls, v: Path) -> Path:
        """Ensure data directories exist with proper permissions"""
        try:
            v.mkdir(parents=True, exist_ok=True)
            os.chmod(v, 0o755)  # Ensure write permissions
            if not v.exists():
                raise RuntimeError(f"Failed to create directory: {v}")
            return v
        except Exception as e:
            logging.error(f"Directory validation failed: {str(e)}")
            raise

    class Config:
        arbitrary_types_allowed = True

config = PipelineConfig()

@task(retries=3, retry_delay_seconds=30, log_prints=True)
async def process_hour_task(
    process_time: datetime,
    s3_bucket: str,
    product: str
) -> bool:
    """Process a single hour using actual ETL pipeline"""
    logger = get_run_logger()
    
    try:
        # Validate processing time
        if process_time > datetime.utcnow() + timedelta(hours=1):
            raise ValueError(f"Cannot process future hour: {process_time}")

        # Set time-specific environment variables
        os.environ.update({
            'GOES_YEAR': process_time.strftime("%Y"),
            'GOES_DOY': process_time.strftime("%j"),
            'GOES_HOUR': process_time.strftime("%H"),
            'S3_BUCKET': s3_bucket,
            'PRODUCT': product
        })

        # Execute ETL pipeline components
        logger.info(f"Starting ETL for {process_time.isoformat()}")
        
        # 1. Extract data from source
        raw_data = etl.source()
        if not raw_data:
            raise ValueError("No data retrieved from source")
            
        # 2. Apply transformations
        transformed_data = etl.transformations(raw_data)
            
        # 3. Load data to destination
        etl.sink(transformed_data)
        
        logger.info(f"Completed ETL for {process_time.isoformat()}")
        return True

    except Exception as e:
        logger.error(f"Hourly ETL failed for {process_time}: {str(e)}")
        return False

def generate_hourly_intervals(
    start: datetime,
    end: datetime,
    selected_hours: Optional[List[int]] = None
) -> List[datetime]:
    """Generate valid processing times within date range"""
    intervals = []
    current = start.replace(minute=0, second=0, microsecond=0)
    end = end.replace(minute=0, second=0, microsecond=0)
    
    while current <= end:
        if selected_hours is None or current.hour in selected_hours:
            intervals.append(current)
        current += timedelta(hours=1)
    
    return intervals

@flow(name="GLM-ETL-Pipeline")
async def etl_ingest(
    start_date: str,
    end_date: str,
    hours: Optional[List[int]] = None,
    s3_bucket: str = 'noaa-goes18',
    product: str = 'GLM-L2-LCFA'
) -> dict:
    """Main ETL orchestration flow with actual pipeline integration"""
    logger = get_run_logger()
    
    try:
        # Validate and parse dates
        start = datetime.strptime(start_date, "%d/%m/%Y")
        end = datetime.strptime(end_date, "%d/%m/%Y")
        
        if start > end:
            raise ValueError("Start date must be before end date")

        # Generate processing schedule
        processing_times = generate_hourly_intervals(start, end, hours)
        if not processing_times:
            raise ValueError("No valid hours in selected date range")
            
        logger.info(f"Processing {len(processing_times)} hours between {start} and {end}")

        # Execute hourly processing concurrently
        results = []
        for pt in processing_times:
            # Submit each task and collect results directly
            result = await process_hour_task.with_options(name=f"ETL-{pt.strftime('%Y%m%d%H')}")(
                process_time=pt,
                s3_bucket=s3_bucket,
                product=product
            )
            results.append(result)
        
        # Calculate success metrics
        success_count = sum(results)
        success_rate = success_count / len(results) if results else 0
        
        return {
            "total_hours": len(results),
            "successful": success_count,
            "success_rate": success_rate,
            "start_time": start.isoformat(),
            "end_time": end.isoformat()
        }

    except Exception as e:
        logger.error(f"ETL orchestration failed: {str(e)}")
        raise

# Analytics Tasks
@task(retries=2)
def preprocessor(data: pd.DataFrame) -> pd.DataFrame:
    """Preprocess data for clustering"""
    logger = get_run_logger()
    try:
        processed_data = data.copy().fillna(0)
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
        return {"data": data, "clusters": clusters, "model": kmeans}
    except Exception as e:
        logger.error(f"Clustering failed: {str(e)}")
        raise

@task(retries=2)
def silhouette_evaluator(cluster_results: Dict[str, Any]) -> float:
    """Evaluate clustering using silhouette score"""
    logger = get_run_logger()
    try:
        score = silhouette_score(cluster_results["data"], cluster_results["clusters"])
        logger.info(f"Silhouette score: {score}")
        return score
    except Exception as e:
        logger.error(f"Silhouette evaluation failed: {str(e)}")
        raise

@task(retries=2)
def elbow_evaluator(cluster_results: Dict[str, Any]) -> Dict[str, Any]:
    """Evaluate clustering using elbow method"""
    logger = get_run_logger()
    try:
        inertias = [KMeans(n_clusters=k, random_state=42).fit(cluster_results["data"]).inertia_ 
                   for k in range(1, 11)]
        return {"inertias": inertias, "optimal_k": np.argmin(np.diff(inertias)) + 1}
    except Exception as e:
        logger.error(f"Elbow evaluation failed: {str(e)}")
        raise

@flow(name="Clustering Analysis", retries=3, retry_delay_seconds=30)
def cluster_analysis() -> Dict[str, Any]:
    """Clustering analysis with enhanced path validation"""
    logger = get_run_logger()
    try:
        data_path = config.PROCESSED_DIR / "processed_data.parquet"
        if not data_path.exists():
            raise FileNotFoundError(f"Missing processed data at {data_path}")
            
        raw_data = pd.read_parquet(data_path)
        processed_data = preprocessor(raw_data)
        
        # Feature safety check
        feature_cols = processed_data.select_dtypes(include=['number']).columns
        if len(feature_cols) == 0:
            raise ValueError("No numeric features available for clustering")
            
        cluster_input = processed_data[feature_cols]
        cluster_results = kmeans_cluster(cluster_input)
        
        # Result persistence
        result_path = config.ANALYTICS_DIR / "clustering_results.parquet"
        pd.DataFrame({
            "cluster": cluster_results["clusters"],
            **{col: cluster_input[col] for col in feature_cols}
        }).to_parquet(result_path)
        
        return {"status": "success", "path": str(result_path)}
        
    except Exception as e:
        logger.error(f"Clustering analysis failed: {str(e)}")
        raise

@flow(name="Main-Orchestration")
async def main_pipeline() -> None:
    """End-to-end pipeline orchestration"""
    logger = get_run_logger()
    
    try:
        # Execute ETL ingestion
        etl_result = await etl_ingest(
            start_date="05/04/2025",
            end_date="05/04/2025",
            hours=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 23]
        )
        
        if etl_result["success_rate"] < 0.95:
            raise ValueError("ETL success rate below acceptable threshold")
            
        # Trigger downstream analysis
        await run_deployment(
            name="ClusterAnalysis/deployment",
            parameters={"source_dir": str(config.PROCESSED_DIR)}
        )
        
        logger.info("Pipeline execution completed successfully")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main_pipeline()) # Add async event loop