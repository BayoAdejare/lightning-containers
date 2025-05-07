# models/risk_assessment.py

import numpy as np
import pandas as pd

from typing import Dict, List, Tuple, Any, Optional
from enum import Enum
import logging 

logger = logging.getLogger(__name__)

class RiskLevel(str, Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MODERATE = "MODERATE"
    LOW = "LOW"
    MINIMAL = "MINIMAL"

class RiskComponents:
    """Component scores based on lightning characteristics."""
    
    def __init__(
        self,
        flash_density: float,
        energy: float,
        duration: float,
    ):
        self.flash_density = round(flash_density, 2)
        self.energy = round(energy, 2)
        self.duration = round(duration, 2)
    
    def to_dict(self) -> Dict[str, float]:
        return {
            "flash_density": self.flash_density,
            "energy": self.energy,
            "duration": self.duration,
        }

class RiskAssessment:
    """Lightning strike risk assessment for a grid region."""
    
    def __init__(
        self,
        overall_score: float,
        components: RiskComponents,
        region_id: str,
        timestamp: pd.Timestamp,
        forecast_period_hours: int = 1  # GLM data is near real-time
    ):
        self.overall_score = round(overall_score, 2)
        self.components = components
        self.region_id = region_id
        self.timestamp = timestamp
        self.forecast_period_hours = forecast_period_hours
        
        # Determine risk level based on overall score
        if self.overall_score > 0.8:
            self.risk_level = RiskLevel.CRITICAL
        elif self.overall_score > 0.6:
            self.risk_level = RiskLevel.HIGH
        elif self.overall_score > 0.4:
            self.risk_level = RiskLevel.MODERATE
        elif self.overall_score > 0.2:
            self.risk_level = RiskLevel.LOW
        else:
            self.risk_level = RiskLevel.MINIMAL
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "overall_score": self.overall_score,
            "components": self.components.to_dict(),
            "risk_level": self.risk_level,
            "region_id": self.region_id,
            "timestamp": self.timestamp.isoformat(),
            "forecast_period_hours": self.forecast_period_hours
        }

class GLMAssessor:
    """Assesses grid risk using NOAA GLM lightning data."""
    
    def __init__(
        self,
        density_weight: float = 0.6,
        energy_weight: float = 0.3,
        duration_weight: float = 0.1,
        critical_density: float = 10.0,  # Flashes/km²/hr
        critical_energy: float = 1e15,   # Joules
        critical_duration: float = 60    # Minutes
    ):
        """
        Initialize with GLM-specific parameters.
        
        Args:
            density_weight: Weight for flash density (0-1)
            energy_weight: Weight for energy content (0-1)
            duration_weight: Weight for event duration (0-1)
            critical_density: Density threshold for maximum score
            critical_energy: Energy threshold for maximum score
            critical_duration: Duration threshold for maximum score
        """
        # Normalize weights to sum to 1.0
        total = density_weight + energy_weight + duration_weight
        self.density_weight = density_weight / total
        self.energy_weight = energy_weight / total
        self.duration_weight = duration_weight / total
        
        self.critical_density = critical_density
        self.critical_energy = critical_energy
        self.critical_duration = critical_duration
        
        logger.info(f"GLM Assessor initialized with weights: "
                   f"Density={self.density_weight:.2f}, "
                   f"Energy={self.energy_weight:.2f}, "
                   f"Duration={self.duration_weight:.2f}")

    def assess_risk(
        self, 
        region_id: str,
        glm_data: Dict[str, float]
    ) -> RiskAssessment:
        """
        Calculate lightning risk score using GLM data.
        
        Args:
            region_id: Grid region identifier
            glm_data: Dictionary with GLM parameters:
                - flash_density: Flashes/km²/hr
                - total_energy: Total optical energy (Joules)
                - duration: Event duration (minutes)
                - centroid_lat: Lightning cluster latitude
                - centroid_lon: Lightning cluster longitude
        
        Returns:
            RiskAssessment object with components
        """
        # Calculate individual risk components
        density_risk = self._calculate_density_risk(glm_data['flash_density'])
        energy_risk = self._calculate_energy_risk(glm_data['total_energy'])
        duration_risk = self._calculate_duration_risk(glm_data['duration'])
        
        # Calculate weighted sum
        overall_score = (
            density_risk * self.density_weight +
            energy_risk * self.energy_weight +
            duration_risk * self.duration_weight
        )
        
        return RiskAssessment(
            overall_score=overall_score,
            components=RiskComponents(
                flash_density=density_risk * self.density_weight,
                energy=energy_risk * self.energy_weight,
                duration=duration_risk * self.duration_weight
            ),
            region_id=region_id,
            timestamp=pd.Timestamp.now(),
            forecast_period_hours=1
        )

    def _calculate_density_risk(self, density: float) -> float:
        """Calculate risk from flash density."""
        return min(density / self.critical_density, 1.0)

    def _calculate_energy_risk(self, energy: float) -> float:
        """Calculate risk from optical energy."""
        return min(np.log10(energy) / np.log10(self.critical_energy), 1.0)

    def _calculate_duration_risk(self, duration: float) -> float:
        """Calculate risk from event duration."""
        return min(duration / self.critical_duration, 1.0)

    def get_risk_factors(self, glm_data: Dict[str, float]) -> Dict[str, float]:
        """Get raw risk factors for visualization."""
        return {
            "Flash Density (flashes/km²/hr)": glm_data['flash_density'],
            "Total Energy (J)": glm_data['total_energy'],
            "Event Duration (min)": glm_data['duration'],
            "Latitude": glm_data['centroid_lat'],
            "Longitude": glm_data['centroid_lon']
        }