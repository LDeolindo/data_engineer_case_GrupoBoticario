import pandas as pd
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator, BigQueryInsertJobOperator)
from airflow.providers.google.cloud.operators.gcs import \
    GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import \
    GCSToBigQueryOperator
from airflow.utils.dates import datetime
from airflow.utils.task_group import TaskGroup

SPOTIFY_TOKEN = Variable.get("SPOTIFY_TOKEN")

BUCKET = "grupoboticario-landing-zone-spotify"
DATASET_ID = "spotify"
TABLE_ID_SEARCH = "spotify_tb_5"
TABLE_ID_EPISODES = "spotify_tb_6"
OBJECT_SEARCH = "search_data_hackers"
OBJECT_EPISODES = "episodes_data_hackers"


def fetch_data_from_spotify_api_search_to_gcs(
    bucket, filename, spotify_endpoint, spotify_params, spotify_token
):
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {spotify_token}"
    }

    res = requests.get(url=spotify_endpoint,
                       params=spotify_params, headers=headers)

    json_object = res.json()
    df = pd.json_normalize(json_object["shows"]["items"])
    df = df[["name", "description", "id", "total_episodes"]]
    df.to_csv(f"gs://{bucket}/{filename}", index=None, header=True)


def fetch_data_from_spotify_api_episodes_to_gcs(
    bucket, filename, spotify_endpoint, spotify_params, spotify_token
):
    df = pd.DataFrame()

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {spotify_token}"
    }

    while (spotify_endpoint):
        res = requests.get(url=spotify_endpoint,
                           params=spotify_params, headers=headers)
        json_object = res.json()
        df = df.append(pd.json_normalize(
            json_object["items"]), ignore_index=True)

        spotify_endpoint = json_object["next"]

    df = df[["id", "name", "description", "release_date",
             "duration_ms", "language", "explicit", "type"]]
    df.to_csv(f"gs://{bucket}/{filename}", index=None, header=True)


default_args = {"owner": "Airflow", "retries": None}

with DAG(
    "spotify_dag",
    start_date=datetime(2023, 4, 7),
    description="DAG responsavel por fazer o ETL no BigQuery dos dados do Spotify.",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:
    begin = EmptyOperator(task_id="begin")

    create_gcs_landing_bucket = GCSCreateBucketOperator(
        task_id="create_gcs_landing_bucket",
        bucket_name=BUCKET,
        storage_class="MULTI_REGIONAL",
    )

    fetch_data_api_search = PythonOperator(
        task_id="fetch_data_from_spotify_api_search",
        python_callable=fetch_data_from_spotify_api_search_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "filename": f"{OBJECT_SEARCH}.csv",
            "spotify_endpoint": "https://api.spotify.com/v1/search",
            "spotify_params": {
                "q": "data hackers",
                "type": "show",
                "limit": "50"
            },
            "spotify_token": SPOTIFY_TOKEN
        },
    )

    fetch_data_api_episodes = PythonOperator(
        task_id="fetch_data_from_spotify_api_episodes",
        python_callable=fetch_data_from_spotify_api_episodes_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "filename": f"{OBJECT_EPISODES}.csv",
            "spotify_endpoint": "https://api.spotify.com/v1/shows/1oMIHOXsrLFENAeM743g93/episodes",
            "spotify_params": {
                "id": "data hackers",
                "limit": "50",
                "offset": "0"
            },
            "spotify_token": SPOTIFY_TOKEN
        },
    )

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id=f"create_dataset_{DATASET_ID}", dataset_id=DATASET_ID
    )

    with TaskGroup(group_id="search") as search_group:
        gcs_to_bq_search = GCSToBigQueryOperator(
            task_id=f"gcs_to_bigquery_{OBJECT_SEARCH}",
            bucket=BUCKET,
            source_objects=f"{OBJECT_SEARCH}.csv",
            destination_project_dataset_table=f"{DATASET_ID}.{TABLE_ID_SEARCH}",
            schema_fields=[
                {'name': 'name', 'type': 'STRING'},
                {'name': 'description', 'type': 'STRING'},
                {'name': 'id', 'type': 'STRING'},
                {'name': 'total_episodes', 'type': 'INTEGER'}
            ],
            write_disposition="WRITE_TRUNCATE",
        )

    with TaskGroup(group_id="episodes") as episodes_group:
        gcs_to_bq_episodes = GCSToBigQueryOperator(
            task_id=f"gcs_to_bigquery_{OBJECT_EPISODES}",
            bucket=BUCKET,
            source_objects=f"{OBJECT_EPISODES}.csv",
            destination_project_dataset_table=f"{DATASET_ID}.{TABLE_ID_EPISODES}",
            schema_fields=[
                {'name': 'id', 'type': 'STRING'},
                {'name': 'name', 'type': 'STRING'},
                {'name': 'description', 'type': 'STRING'},
                {'name': 'release_date', 'type': 'DATE'},
                {'name': 'duration_ms', 'type': 'INTEGER'},
                {'name': 'language', 'type': 'STRING'},
                {'name': 'explicit', 'type': 'BOOL'},
                {'name': 'type', 'type': 'STRING'}
            ],
            write_disposition="WRITE_TRUNCATE",
        )

        bq_episodes_gp = BigQueryInsertJobOperator(
            task_id="select_grupoboticario-only",
            configuration={
                "query": {
                    "query": "CALL procedures_spotify.insert_query_table_07()",
                    "useLegacySql": False,
                }
            },
        )

        chain(gcs_to_bq_episodes, bq_episodes_gp)

    end = EmptyOperator(task_id="end")

    chain(
        begin,
        create_gcs_landing_bucket,
        [fetch_data_api_search, fetch_data_api_episodes],
        create_dataset,
        [search_group, episodes_group],
        end
    )
