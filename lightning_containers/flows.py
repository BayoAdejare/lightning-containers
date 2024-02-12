from prefect import flow
from tasks.etl import source, transformations, sink
from tasks.clustering import (
    preprocessor,
    kmeans_cluster,
    Silhouette_evaluator,
    elbow_evaluator,
)


@flow(retries=3, retry_delay_seconds=15, log_prints=True)
def etl_ingest():
    s = source()
    t = transformations(s)
    s = sink(t)
    return "ETL Flow completed!"


@flow(retries=3, retry_delay_seconds=15, log_prints=True)
def cluster_analysis():
    p = preprocessor()
    k = kmeans_cluster(p)
    try:
        s = Silhouette_evaluator(k)
    except:
        e = elbow_evaluator(k)
    return "Clustering Flow completed!"


@flow(retries=2, retry_delay_seconds=5, log_prints=True)
def dashboard_refresh():
    return "Dashboard Flow completed!"


if __name__ == "__main__":
    etl_ingest()
    cluster_analysis()
    dashboard_refresh()
