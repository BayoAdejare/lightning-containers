from prefect import flow
from tasks import *
from tasks.analytics import (
    preprocessor,
    kmeans_cluster,
    Silhouette_evaluator,
    elbow_evaluator,
)

start_date = "10/01/2024"
end_date = "10/03/2024"


# 24 hours :test:
hours = [
    "00",
    "01",
    "02",
    "03",
    "04",
    "05",
    "06",
    "07",
    "08",
    "09",
    "10",
    "11",
    "12",
    "13",
    "14",
    "15",
    "16",
    "17",
    "18",
    "19",
    "20",
    "21",
    "22",
    "23",
]


@flow(retries=3, retry_delay_seconds=15, log_prints=True)
def etl_ingest():
    print(f"Starting ingestion from {start_date} to {end_date}..")
    ingestion(start_date=start_date, end_date=end_date, hours=hours)
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
