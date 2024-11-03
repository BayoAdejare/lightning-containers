from datetime import datetime, timedelta
from typing import List, Optional, Tuple, Dict, Any
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from pathlib import Path
import pandas as pd
from tasks.analytics import (
    preprocessor,
    kmeans_cluster,
    silhouette_evaluator,
    elbow_evaluator,
)

class PipelineConfig:
    # Base configuration remains the same...
    BASE_DIR = Path(__file__).parent.resolve()
    DATA_DIR = BASE_DIR / "data"
    LOAD_DIR = DATA_DIR / "Load"
    PROCESSED_DIR = DATA_DIR / "Processed"
    ANALYTICS_DIR = DATA_DIR / "Analytics"
    
    DEFAULT_RETRY_DELAYS = [30, 60, 120]
    DEFAULT_CACHE_TTL = "1h"
    DEFAULT_HOURS = [str(x).zfill(2) for x in range(24)]

    @classmethod
    def initialize_directories(cls) -> None:
        """Create all required directories if they don't exist"""
        directories = [
            cls.DATA_DIR,
            cls.LOAD_DIR,
            cls.PROCESSED_DIR,
            cls.ANALYTICS_DIR
        ]
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)

@flow(name="Clustering Analysis",
      retries=3,
      retry_delay_seconds=30)
def cluster_analysis() -> Dict[str, Any]:
    """Clustering analysis flow with proper error handling and data persistence"""
    logger = get_run_logger()
    logger.info("Starting clustering analysis")
    
    try:
        # Load data from processed directory
        data_path = PipelineConfig.PROCESSED_DIR / "processed_data.parquet"
        if not data_path.exists():
            raise FileNotFoundError(f"Processed data not found at {data_path}")
            
        raw_data = pd.read_parquet(data_path)
        
        # Preprocess
        processed_data = preprocessor(raw_data)
        if processed_data.empty:
            raise ValueError("No data available for clustering")
        
        # Perform clustering
        cluster_results = kmeans_cluster(processed_data)
        
        # Evaluate results
        results = {}
        try:
            results["silhouette_score"] = silhouette_evaluator(cluster_results)
            logger.info(f"Silhouette analysis completed: {results['silhouette_score']}")
        except Exception as e:
            logger.warning(f"Silhouette analysis failed, falling back to elbow method: {str(e)}")
            results["elbow_analysis"] = elbow_evaluator(cluster_results)
            logger.info(f"Elbow analysis completed: {results['elbow_analysis']}")
        
        # Save results
        results_path = PipelineConfig.ANALYTICS_DIR / "clustering_results.parquet"
        pd.DataFrame({
            "cluster": cluster_results["clusters"],
            **{f"feature_{i}": processed_data.iloc[:, i] 
               for i in range(processed_data.shape[1])}
        }).to_parquet(results_path)
        
        logger.info(f"Clustering results saved to {results_path}")
        return results
        
    except Exception as e:
        logger.exception(f"Clustering analysis failed: {str(e)}")
        raise

@flow(name="Main Pipeline")
def main() -> None:
    """Main pipeline with proper analytics integration"""
    logger = get_run_logger()
    
    try:
        # Initialize environment
        PipelineConfig.initialize_directories()
        
        # Run ETL
        etl_results = etl_ingest(
            start_date="09/10/2024",
            end_date="09/10/2024",
            hours=["00", "01", "02"]
        )
        
        # Run clustering if ETL successful
        if etl_results:
            clustering_results = cluster_analysis()
            logger.info(f"Clustering completed with results: {clustering_results}")
            
        # Update dashboard
        dashboard_refresh()
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()
