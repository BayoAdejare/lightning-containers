import os

import duckdb as db
import pandas as pd

from prefect import task, get_run_logger
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
def preprocessor(df):
    logger = get_run_logger()
    # config data load
    # df = db_connect("preprocess")
    logger.info(f"Starting file extracts for historical outages: {df}")
    results = []
    
    # Load FIPS code to coordinate mapping
    try:
    # Load from authoritative source
        fips_df = pd.read_csv("src/data/Raw/CenPop2020_Mean_CO.txt", 
                            dtype={'STATEFP': str, 'COUNTYFP': str})
        fips_df['FIPS'] = fips_df['STATEFP'] + fips_df['COUNTYFP']
        fips_df = fips_df[['FIPS', 'LATITUDE', 'LONGITUDE']].rename(
            columns={'LATITUDE': 'latitude', 'LONGITUDE': 'longitude'}
        )
    except Exception as e:
        st.error(f"Failed to load FIPS coordinates: {str(e)}")
    
    # Process outage data
    if not df.empty:
        # Convert FIPS to string with leading zeros
        df['fips'] = df['fips'].astype(str).str.zfill(5)
        
        # Merge with coordinates
        df = df.merge(
            fips_df,
            left_on='fips',
            right_on='FIPS',
            how='left'
        )

    return df # results


@task(
    name="Cluster",
    description="Group data into 'k' clusters.",
    retries=3,
    retry_delay_seconds=15,
)
def kmeans_cluster(preprocessor: pd.DataFrame):
    k = int(os.getenv("NUM_OF_CLUSTERS", 12))
    logger.info(f"Starting cluster model, k={k}...")
    results = []
    clusters = kmeans_model(preprocessor, k)
    results = pd.DataFrame(clusters)
    logger.info(f"Generated cluster model ...")
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
    logger.info(f"Starting silhouette evaluation ...")
    sil_coefficients = sil_evaluation(kmeans_cluster)
    results = sil_coefficients.set_index("k", drop=True)
    k_max = results["silhouette_coefficient"].argmax()
    logger.info(f"Silhoutte coefficients: {results}")
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
    logger.info(f"Starting elbow evaluation: {kmeans_cluster}")
    results = []
    elb_sse = elb_evaluation(kmeans_cluster)
    results.append(elb_sse)
    logger.info(f"Elbow SSE ...")
    # todo: save evaluations db
    return results
