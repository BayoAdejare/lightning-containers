import pandas as pd

from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score


def preprocess(df: pd.DataFrame) -> pd.DataFrame:
    """Preprocess the data"""
    # filter for only weather related
    df = df[df["Event Type"].str.contains("Weather", na=False)]
    geo_df = df
    return geo_df


def kmeans_model(data: pd.DataFrame, num_clusters: int):
    """
    Fit data to kmeans cluster algorithm.
    """
    X = data.loc[:, ["longitude", "latitude"]]

    kmeans_kwargs = {
        "init": "k-means++",
        "n_init": 10,
        "max_iter": 100,
        "random_state": 60,
    }

    kmeans = KMeans(n_clusters=num_clusters, **kmeans_kwargs)
    X["Cluster"] = kmeans.fit_predict(X)
    X["Cluster"] = X["Cluster"].astype("category")
    return X


def sil_evaluation(data: pd.DataFrame):
    """
    Evaluate the k-means silhouette coefficient.
    """
    data = data.loc[:, ["longitude", "latitude"]]

    kmeans_kwargs = {
        "init": "k-means++",
        "n_init": 10,
        "max_iter": 50,
        "random_state": 60,
    }

    # holds the silhouette coefficients for each k
    silhouette_coefficients = {}

    # start at 2 clusters for silhouette coefficient
    for k in range(2, 24):
        kmeans = KMeans(n_clusters=k, **kmeans_kwargs)
        kmeans.fit(data)
        score = silhouette_score(data, kmeans.labels_)
        silhouette_coefficients[k] = score

    sil_df = pd.DataFrame(
        list(silhouette_coefficients.items()), columns=["k", "silhouette_coefficient"]
    )

    return sil_df


def elb_evaluation(data: pd.DataFrame):
    """
    Evaluate the k-means elbow method, sum of squared error.
    """

    kmeans_kwargs = {
        "init": "k-means++",
        "n_init": 10,
        "max_iter": 50,
        "random_state": 60,
    }

    # A list holds the sum of squared distance for each k
    elb_sse = []

    # Return SSE for each k
    for k in range(1, 24):
        kmeans = KMeans(n_clusters=k, **kmeans_kwargs)
        kmeans.fit(data)
        elb_sse.append(kmeans.inertia_)

    return elb_sse
