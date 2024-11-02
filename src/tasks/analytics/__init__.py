import os

import sqlite3 as db
import pandas as pd

from prefect import task
from .clustering import preprocess, kmeans_model, sil_evaluation, elb_evaluation
from datetime import datetime, timedelta
from pathlib import Path

# Date range
dt = datetime.utcnow() - timedelta(hours=1)
start_date = str(dt)
end_date = str(dt)
hours = dt.strftime("%H")


def db_connect(process: str):
    basepath = Path(__file__).resolve().parent.parent.parent.parent
    load_path = Path("data/Load")
    dest_folder = os.path.join(basepath, load_path)

    os.chdir(dest_folder)
    db_path = os.path.join(dest_folder, "glmFlash.db")

    if process == "preprocess":
        # conn string for preprocess data
        conn = db.connect(db_path)
        df = pd.read_sql_query("SELECT * FROM tbl_flash", conn)
        return df

    elif process == "model":
        # conn string for model data
        try:
            # create the db connection
            conn = db.connect(db_path)
        except Exception as db_err:
            # connection error
            print(f"Connection error on: {db_path}")
        return conn


@task(
    name="Preprocess",
    description="Preprocess data.",
    retries=3,
    retry_delay_seconds=15,
)
def preprocessor():
    # config data load
    df = db_connect("preprocess")
    print("Starting file extracts for glm ...")
    results = []
    preprocessing = preprocess(df)
    results = pd.DataFrame(preprocessing)
    return results


@task(
    name="Cluster",
    description="Group data into 'k' clusters.",
    retries=3,
    retry_delay_seconds=15,
)
def kmeans_cluster(preprocessor: pd.DataFrame):
    k = int(os.getenv("NUM_OF_CLUSTERS", 12))
    print(f"Starting cluster model, k={k}...")
    results = []
    clusters = kmeans_model(preprocessor, k)
    results = pd.DataFrame(clusters)
    print(f"Generated cluster model ...")
    # save clusters to db
    conn = db_connect("model")
    results.to_sql("results", conn, if_exists="append", index=False)
    return results


@task(
    name="Sil Evaluate",
    description="Silhouette coefficient score 'k'.",
    retries=3,
    retry_delay_seconds=15,
)
def silhouette_evaluator(kmeans_cluster: pd.DataFrame):
    print(f"Starting silhouette evaluation ...")
    sil_coefficients = sil_evaluation(kmeans_cluster)
    results = sil_coefficients.set_index("k", drop=True)
    k_max = results["silhouette_coefficient"].argmax()
    print(f"Silhoutte coefficients: {results}")
    os.environ["NUM_OF_CLUSTERS"] = str(k_max)
    # todo: save evaluations db
    return results


@task(
    name="Elb Evaluate",
    description="Elbow method score 'k'.",
    retries=3,
    retry_delay_seconds=15,
)
def elbow_evaluator(kmeans_cluster: pd.DataFrame):
    print(f"Starting elbow evaluation: {kmeans_cluster}")
    results = []
    elb_sse = elb_evaluation(kmeans_cluster)
    results.append(elb_sse)
    print(f"Elbow SSE ...")
    # todo: save evaluations db
    return results
