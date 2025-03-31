import os
import shutil
import warnings
from typing import List, Tuple, Optional
from pathlib import Path
from datetime import datetime, timedelta
from prefect import task, get_run_logger
from botocore import UNSIGNED
from botocore.client import Config
from boto3 import client
from tqdm import tqdm
from pydantic import BaseModel, field_validator

from .extract import extract_s3
from .transform import transform_file
from .load import load_tbl

warnings.simplefilter("ignore")

class PipelineConfig(BaseModel):
    """Centralized pipeline configuration with path validation"""
    BASE_DIR: Path = Path(__file__).resolve().parent.parent.parent.parent
    EXTRACT_DIR: Path = BASE_DIR / "data" / "Extract"
    TRANSFORM_DIR: Path = BASE_DIR / "data" / "Transform"
    LOAD_DIR: Path = BASE_DIR / "data" / "Load"
    
    @field_validator('EXTRACT_DIR', 'TRANSFORM_DIR', 'LOAD_DIR')
    @classmethod
    def validate_dirs(cls, v: Path) -> Path:
        """Ensure directories exist with proper permissions"""
        try:
            v.mkdir(parents=True, exist_ok=True)
            v.chmod(0o755)  # rwxr-xr-x
            if not v.exists():
                raise RuntimeError(f"Failed to create directory: {v}")
            return v
        except Exception as e:
            raise ValueError(f"Directory validation failed for {v}: {str(e)}")

    class Config:
        arbitrary_types_allowed = True

config = PipelineConfig()

def etl_config(process: str) -> Tuple[str, str, Path]:
    """Get ETL configuration with validated paths"""
    dt = datetime.utcnow() - timedelta(hours=3)
    params = {
        "year": os.getenv("GOES_YEAR", dt.strftime("%Y")),
        "day_of_year": os.getenv("GOES_DOY", dt.strftime("%j")),
        "hour": os.getenv("GOES_HOUR", dt.strftime("%H")),
        "bucket": os.getenv("S3_BUCKET", "noaa-goes17"),
        "product": os.getenv("PRODUCT", "GLM-L2-LCFA")
    }
    
    prefix = f"{params['product']}/{params['year']}/{params['day_of_year']}/{params['hour']}/"
    
    if process == "extract":      
        return prefix, params['bucket'], config.EXTRACT_DIR
    elif process == "transform":
        return str(config.EXTRACT_DIR), params['bucket'], config.TRANSFORM_DIR
    elif process == "load":
        return str(config.TRANSFORM_DIR), params['bucket'], config.LOAD_DIR
    raise ValueError(f"Invalid process: {process}")

@task(name="Source extract", retries=2, retry_delay_seconds=3)
def source() -> List[Path]:
    """Secure S3 file extraction with path validation"""
    logger = get_run_logger()
    prefix, bucket_name, extract_dir = etl_config("extract")
    
    try:
        s3 = client("s3", config=Config(signature_version=UNSIGNED))
        objects = s3.list_objects(Bucket=bucket_name, Prefix=prefix).get('Contents', [])
        downloaded_files = []
        for obj in tqdm(objects, desc=f"Extracting {prefix}", ascii=" >="):
            try:
                file_path = extract_dir / Path(obj['Key']).name
                s3.download_file(bucket_name, obj['Key'], str(file_path))
                downloaded_files.append(file_path)
                logger.debug(f"Downloaded {file_path.name}")
            except Exception as e:
                logger.error(f"Failed to download {obj['Key']}: {str(e)}")
                raise
                
        return downloaded_files
        
    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}")
        raise

@task(name="Transform data", retries=2, retry_delay_seconds=30)
def transformations(source_files: Optional[List[Path]] = None) -> List[Path]:
    """Robust file transformation with proper path handling"""
    logger = get_run_logger()
    
    # Get configuration from validated source
    transform_dir = config.TRANSFORM_DIR
    extract_dir = config.EXTRACT_DIR

    # Clean output directory first
    for f in transform_dir.glob("*.csv"):
        f.unlink(missing_ok=True)

    # Get source files if not provided
    source_files = source_files or list(extract_dir.glob("*.nc"))
    
    logger.info(f"Processing {len(source_files)} files from {extract_dir}")

    transformed_files = []
    for src_path in tqdm(source_files, desc="Transforming"):
        try:
            if not src_path.exists():
                logger.error(f"Missing source: {src_path}")
                continue

            # Process actual source file
            result = transform_file(
                extract_file=src_path,  # Correct source path
                transform_folder=transform_dir,
                filename=src_path.stem,
                chunk_size=10000
            )
            
            if result and result.exists():
                transformed_files.append(result)
                logger.debug(f"Created {result.name}")
            else:
                logger.warning(f"Failed to process {src_path.name}")

        except Exception as e:
            logger.error(f"Error processing {src_path.name}: {str(e)}")
            continue  # Process remaining files

    logger.info(f"Success rate: {len(transformed_files)}/{len(source_files)}")
    return transformed_files

@task(name="Sink load", retries=2, retry_delay_seconds=3)
def sink(transformed_files: List[Path]) -> bool:
    """Secure data loading with transaction safety"""
    logger = get_run_logger()
    transform_dir, bucket_name, load_dir = etl_config("load")

    sink_files = list(Path(config.TRANSFORM_DIR).glob("*.csv"))
    
    try:
        # Stage files first
        stage_dir = load_dir / "stage"
        stage_dir.mkdir(exist_ok=True)
        
        for src_file in sink_files:
            try:
                if not src_file.exists():
                    raise FileNotFoundError(f"Transformed file missing: {src_file}")
                    
                dest_file = stage_dir / src_file.name
                shutil.copy(src_file, dest_file)
                logger.debug(f"Staged {dest_file.name}")
                
            except Exception as e:
                logger.error(f"Failed to stage {src_file.name}: {str(e)}")
                raise
                
        # Atomic move from stage to final location
        for staged_file in stage_dir.glob("*"):
            final_path = load_dir / staged_file.name
            staged_file.rename(final_path)
            
        # Cleanup stage directory
        stage_dir.rmdir()
        
        # Load to database
        load_results = load_tbl(load_dir)
        return True
        
    except Exception as e:
        logger.error(f"Loading failed: {str(e)}")
        # Rollback staged files
        if stage_dir.exists():
            shutil.rmtree(stage_dir)
        raise