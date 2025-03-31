"""
test_clustering.py - Unit tests for clustering functionality

Tests include:
- Basic clustering validation
- Edge cases
- Input/output shape verification
- Metric validation
"""

import numpy as np
import pytest
from sklearn.cluster import KMeans, DBSCAN
from sklearn.datasets import make_blobs
from sklearn.metrics import silhouette_score

@pytest.fixture(scope="module")
def synthetic_data():
    """Generate synthetic clustered data for testing"""
    np.random.seed(42)
    centers = [[1, 1], [-1, -1], [1, -1]]
    X, y = make_blobs(
        n_samples=300,
        centers=centers,
        cluster_std=0.5,
        random_state=42
    )
    return X, y

def test_kmeans_clustering(synthetic_data):
    """Test basic KMeans clustering functionality"""
    X, _ = synthetic_data
    n_clusters = 3
    
    # Initialize and fit model
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    labels = kmeans.fit_predict(X)
    
    # Verify number of clusters
    unique_labels = np.unique(labels)
    assert len(unique_labels) == n_clusters
    
    # Verify cluster centers shape
    assert kmeans.cluster_centers_.shape == (n_clusters, X.shape[1])
    
    # Verify labels shape
    assert labels.shape == (X.shape[0],)

def test_dbscan_clustering(synthetic_data):
    """Test DBSCAN clustering with synthetic data"""
    X, _ = synthetic_data
    dbscan = DBSCAN(eps=0.5, min_samples=5)
    labels = dbscan.fit_predict(X)
    
    # Number of clusters (excluding noise)
    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    assert n_clusters >= 2  # At least 2 real clusters
    
    # Verify noise points identification
    assert np.sum(labels == -1) < X.shape[0] * 0.1  # <10% noise
    
    # Core sample indices check
    core_samples_mask = np.zeros_like(labels, dtype=bool)
    core_samples_mask[dbscan.core_sample_indices_] = True
    assert core_samples_mask.sum() > 0

def test_clustering_quality(synthetic_data):
    """Validate clustering quality metrics"""
    X, _ = synthetic_data
    kmeans = KMeans(n_clusters=3, random_state=42, n_init=10)
    labels = kmeans.fit_predict(X)
    
    # Silhouette score validation
    score = silhouette_score(X, labels)
    assert score > 0.6  # Good cluster separation
    
    # Inertia validation
    inertia = kmeans.inertia_
    assert 100 < inertia < 300  # Expected range for our synthetic data

def test_input_validation():
    """Test invalid input handling"""
    with pytest.raises(ValueError):
        KMeans(n_clusters=0).fit(np.random.rand(10, 2))
        
    # Test non-finite values
    X_invalid = np.array([[1, 2], [np.nan, 3]])
    with pytest.raises(ValueError):
        KMeans().fit(X_invalid)

def test_feature_consistency(synthetic_data):
    """Verify feature dimension consistency"""
    X, _ = synthetic_data
    model = KMeans(n_clusters=3, random_state=42, n_init=10).fit(X)
    
    # Test prediction with mismatched features
    with pytest.raises(ValueError):
        model.predict(np.random.rand(5, 3))  # Original features are 2D

if __name__ == "__main__":
    pytest.main(["-v", __file__])