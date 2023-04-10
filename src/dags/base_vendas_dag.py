from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator, BigQueryInsertJobOperator)
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator, GCSDeleteBucketOperator, GCSDeleteObjectsOperator,
    GCSFileTransformOperator)
from airflow.providers.google.cloud.sensors.bigquery import \
    BigQueryTableExistenceSensor
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import \
    GCSToBigQueryOperator
from airflow.utils.dates import datetime
from airflow.utils.task_group import TaskGroup

BUCKET = "grupoboticario-landing-zone-vendas"
BUCKET_PROCESSING = "grupoboticario-processing-zone-vendas"
PREFIX = "base_vendas"
PROJECT_ID = "case-grupo-boticario"
DATASET_PROCESSING_ZONE = "processing_zone_base_vendas"
DATASET_REFINED_ZONE = "refined_zone_base_vendas"
TABLE_PROCESSING_ZONE = "p_base_vendas"

check_dict = {
    "base_2017": f"{PREFIX}/Base 2017 (1).xlsx",
    "base_2018": f"{PREFIX}/Base_2018 (1).xlsx",
    "base_2019": f"{PREFIX}/Base_2019 (2).xlsx",
}

procedures_dict = {
    "table_01": "procedures_base_vendas.insert_query_table_01()",
    "table_02": "procedures_base_vendas.insert_query_table_02()",
    "table_03": "procedures_base_vendas.insert_query_table_03()",
    "table_04": "procedures_base_vendas.insert_query_table_04()"
}

default_args = {"owner": "Airflow", "retries": None}

with DAG(
    "base_vendas_dag",
    start_date=datetime(2023, 4, 7),
    description="DAG responsavel por fazer o ETL no BigQuery dos dados base de vendas.",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
) as dag:
    begin = EmptyOperator(task_id="begin")

    with TaskGroup(group_id="object_existence_checks") as object_existence_check_group:
        for key, value in check_dict.items():
            GCSObjectExistenceSensor(
                task_id=f"check_file_{key}", bucket=BUCKET, object=value
            )

    create_dataset_processing = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset_processing", dataset_id=DATASET_PROCESSING_ZONE
    )

    with TaskGroup(group_id="load_data_from_gcs_to_bq") as load_data_group:
        create_gcs_processing_bucket = GCSCreateBucketOperator(
            task_id="create_gcs_processing_bucket",
            bucket_name=BUCKET_PROCESSING,
            storage_class="MULTI_REGIONAL",
        )

        sub_groups = []
        for key, object in check_dict.items():
            prefix = "processing_zone/base_vendas"

            with TaskGroup(group_id=f"file_{key}") as load_data_sub_group:
                gcs_transform = GCSFileTransformOperator(
                    task_id=f"transform_xlsx_to_csv",
                    source_bucket=BUCKET,
                    source_object=object,
                    destination_bucket=BUCKET,
                    destination_object=f"{prefix}/{key}.csv",
                    transform_script=[
                        "python",
                        "/home/airflow/gcs/dags/scripts/xlsx_to_csv.py",
                    ],
                )
                gcs_to_bq = GCSToBigQueryOperator(
                    task_id=f"gcs_to_bigquery",
                    bucket=BUCKET,
                    source_objects=f"{prefix}/{key}.csv",
                    destination_project_dataset_table=f"{DATASET_PROCESSING_ZONE}.{TABLE_PROCESSING_ZONE}",
                    write_disposition="WRITE_APPEND",
                )

                chain(gcs_transform, gcs_to_bq)
                sub_groups.append(load_data_sub_group)

        delete_gcs_processing_bucket = GCSDeleteBucketOperator(
            task_id=f"delete_gcs_processing_bucket",
            bucket_name=BUCKET_PROCESSING,
            force=True
        )

        chain(create_gcs_processing_bucket, sub_groups, delete_gcs_processing_bucket)

    check_table_vendas_exists = BigQueryTableExistenceSensor(
        task_id=f"check_for_table_{TABLE_PROCESSING_ZONE}",
        project_id=PROJECT_ID,
        dataset_id=DATASET_PROCESSING_ZONE,
        table_id=TABLE_PROCESSING_ZONE,
    )

    create_dataset_refined = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset_refined", dataset_id=DATASET_REFINED_ZONE
    )

    with TaskGroup(group_id="transform_load_data") as transform_load_data_group:
        for key, procedure in procedures_dict.items():
            BigQueryInsertJobOperator(
                task_id=key,
                configuration={
                    "query": {
                        "query": f"CALL {procedure}",
                        "useLegacySql": False,
                    }
                },
            )

    end = EmptyOperator(task_id="end")

    chain(
        begin,
        object_existence_check_group,
        create_dataset_processing,
        load_data_group,
        check_table_vendas_exists,
        create_dataset_refined,
        transform_load_data_group,
        end,
    )
