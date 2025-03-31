# Lightning Containers Documentation
## Overview

Lightning Containers is a comprehensive data engineering solution for analyzing and visualizing atmospheric lightning data from NOAA's Geostationary Lightning Mapper (GLM). The project combines ETL processes, geospatial analysis, and interactive visualization in a containerized environment.

## System Architecture

### Core Components
1. **Data Pipeline (Backend)**
   - ETL processes for data ingestion
   - Clustering analysis for lightning pattern detection
   - Prefect server for workflow orchestration
   - SQLite with Spatialite extension for geospatial data storage

2. **Visualization Layer (Frontend)**
   - Streamlit dashboard for interactive data exploration
   - GIS visualization capabilities
   - Real-time data monitoring

### Technical Stack
- **Container Platform**: Docker
- **Orchestration**: Prefect
- **Data Processing**: Pandas
- **Storage**: SQLite + Spatialite
- **Frontend**: Streamlit
- **Version Control**: Git
- **CI/CD**: GitHub Actions

## System Requirements

### Minimum Requirements
- CPU: 2 cores
- RAM: 6GB
- Storage: 8GB

### Recommended Specifications
- CPU: 4+ cores
- RAM: 16GB
- Storage: 24GB

## Installation Guide

### Docker Installation (Recommended)
```bash
# Clone repository
git clone https://github.com/BayoAdejare/lightning-containers.git
cd lightning-containers

# Start containers
docker compose up
```

### Local Installation
```bash
# Create and activate virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Start Prefect server
prefect server start

# Start application
python src/flows.py  # Backend
streamlit run app/dashboard.py  # Frontend
```

## Data Pipeline Architecture

### ETL Flow
The ETL (Extract, Transform, Load) pipeline processes NOAA GOES-R GLM lightning data through three main stages:

1. **Extract Stage**
   - Source: AWS S3 buckets containing NOAA GLM data
   - Format: netCDF4 files
   - Process: Downloads raw data based on specified time window

2. **Transform Stage**
   - Converts netCDF4 files to time series CSV format
   - Processes geospatial coordinates
   - Handles data cleaning and normalization

3. **Load Stage**
   - Destination: SQLite database with Spatialite extension
   - Enables geospatial queries and analysis
   - Maintains data persistence

### Clustering Flow
The clustering pipeline implements advanced analytics:

1. **Preprocessing**
   - Data cleaning
   - Feature normalization
   - Preparation for clustering

2. **K-Means Clustering**
   - Groups lightning events based on spatial and temporal patterns
   - Implements configurable clustering parameters

3. **Model Evaluation**
   - Silhouette coefficient analysis
   - Elbow method for optimal cluster determination
   - Performance metrics tracking

## Data Model

### Lightning Data Hierarchy
The system processes three main data entities:
1. **Events**: Individual lightning detections
2. **Groups**: Clustered events
3. **Flashes**: Aggregated groups

### Key Attributes
- Temporal information
- Geospatial coordinates
- Lightning intensity metrics
- Cluster assignments

## Dashboard Features

### Interactive Map
- Real-time visualization of lightning events
- Cluster visualization
- Temporal playback capabilities
- Customizable layers

### Analytics Panel
- Time series analysis
- Cluster statistics
- Geospatial patterns
- Custom filtering options

## Development Guide

### Testing
```bash
# Run test suite
pytest

# Test specific components
pytest tests/test_extract.py
pytest tests/test_clustering.py
```

### CI/CD Pipeline
The project implements automated workflows via GitHub Actions:
- Automated testing on pull requests
- Scheduled data quality checks
- Model deployment automation
- Docker image builds

## Troubleshooting

### Common Issues
1. **Insufficient Memory**
   - Solution: Adjust Docker memory allocation
   - Workaround: Process smaller data batches

2. **Database Connection Issues**
   - Check Spatialite extension installation
   - Verify database permissions
   - Confirm port availability

3. **Pipeline Failures**
   - Monitor Prefect UI for error logs
   - Check data source availability
   - Verify AWS credentials if applicable

## Best Practices

### Data Processing
- Use time windowing for large datasets
- Implement error handling for data gaps
- Regular database maintenance

### Development
- Follow project structure conventions
- Document code changes
- Write tests for new features
- Use meaningful commit messages

## Support and Resources

### Official Documentation
- [Project Repository](https://github.com/BayoAdejare/lightning-containers)
- [NOAA GLM Documentation](https://www.goes-r.gov/products/baseline-lightning-detection.html)
- [Prefect Documentation](https://docs.prefect.io/)
- [Streamlit Documentation](https://docs.streamlit.io/)

### Contributing
Refer to CONTRIBUTING.md for:
- Code style guidelines
- Pull request process
- Development setup
- Community guidelines

### License
This project is licensed under the Apache 2.0 License. See LICENSE file for details.
