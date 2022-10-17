"""Aggregate data."""

from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator, DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator, DataprocSubmitPySparkJobOperator)
from airflow.utils import trigger_rule

default_args = {
    "owner": "7506",
    "start_date": datetime(2022, 5, 1),
    "retries": 1,
    "depends_on_past": False,
}

dag = DAG(
    "predict_yellow",
    description="Train a predictor of yellow hail qty per location and hour",
    default_args=default_args,
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    is_paused_upon_creation=True
)

NUM_WORKERS = 2
VCPUS_PER_WORKER = 2
PARALLELISM_MULTIPLIER = 3

project_id = "clase-spark-365815"
cluster_name = "pred-yellow-data"

CLUSTER_CONFIG = ClusterGenerator(
    project_id=project_id,
    num_masters=1,
    num_workers=NUM_WORKERS,
    master_machine_type="n1-standard-2",
    worker_machine_type="n1-standard-2",
    worker_disk_size=50,
    master_disk_size=50,
    zone="us-west1-b",
    subnetwork_uri="default",
    properties={
        "spark:spark.default.parallelism": str(
            NUM_WORKERS * VCPUS_PER_WORKER * PARALLELISM_MULTIPLIER
        )
    },
    enable_component_gateway=True,
).make()

create_dataproc_cluster = DataprocCreateClusterOperator(
    task_id="create_data_pred_dataproc_cluster",
    project_id=project_id,
    cluster_name=cluster_name,
    cluster_config=CLUSTER_CONFIG,
    region="us-west1",
    dag=dag,
)

submit_job = DataprocSubmitPySparkJobOperator(
    task_id="submit_data_pred_task",
    job_name="pyspark_yellow_pred_task",
    project_id=project_id,
    cluster_name=cluster_name,
    main="gs://7506-spark/jobs/predict_qty_by_location.py",
    arguments=[
        "--bucket-name",
        "7506-nyc-taxi",
        "--artifacts-bucket-name",
        "7506-spark",
    ],
    region="us-west1",
    files=["gs://7506-spark/jars/gcs-connector-hadoop3-latest.jar"],
    dag=dag,
)

delete_dataproc_cluster = DataprocDeleteClusterOperator(
    task_id="delete_data_pred_dataproc_cluster",
    project_id=project_id,
    region="us-west1",
    cluster_name=cluster_name,
    trigger_rule=trigger_rule.TriggerRule.ALL_DONE,
    dag=dag,
)

create_dataproc_cluster >> submit_job >> delete_dataproc_cluster
