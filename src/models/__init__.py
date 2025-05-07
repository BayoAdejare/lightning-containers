from typing import Dict, List, Tuple, Any, Optional

from prefect import task, get_run_logger

@task(name="Risk assessment", retries=2, retry_delay_seconds=3)
def generate_glm_report(region_id: str, glm_data: Dict[str, float]) -> Dict[str, Any]:
    """Generate a risk report using GLM data."""
    assessor = risk_assessment.GLMAssessor()
    assessment = assessor.assess_risk(region_id, glm_data)
    
    return {
        "assessment": assessment.to_dict(),
        "glm_metrics": assessor.get_risk_factors(glm_data),
        "timestamp": pd.Timestamp.now().isoformat()
    }

# Example GLM data input
SAMPLE_GLM_DATA = {
    "flash_density": 8.2,       # Flashes/km²/hr
    "total_energy": 5e14,       # Joules
    "duration": 45,             # Minutes
    "centroid_lat": 39.7392,    # Degrees
    "centroid_lon": -104.9903   # Degrees
}