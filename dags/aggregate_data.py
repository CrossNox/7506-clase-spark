"""Aggregate data."""

from datetime import datetime

from airflow import DAG
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStoragePrefixSensor
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator, DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator, DataprocSubmitPySparkJobOperator)
from airflow.utils import trigger_rule

default_args = {
    "owner": "7506",
    "start_date": datetime(2011, 1, 1),
    "retries": 1,
    "depends_on_past": False,
}

dag = DAG(
    "aggregate_data",
    description="Monthly yellow data analytics aggregation",
    default_args=default_args,
    schedule_interval="@monthly",
    max_active_runs=1,
    catchup=True,
)

NUM_WORKERS = 3
VCPUS_PER_WORKER = 2
PARALLELISM_MULTIPLIER = 3

project_id = "datos-350705"
cluster_name = "agg-yellow-data"

CLUSTER_CONFIG = ClusterGenerator(
    project_id=project_id,
    num_masters=1,
    num_workers=NUM_WORKERS,
    master_machine_type="n1-standard-2",
    worker_machine_type="n1-standard-2",
    worker_disk_size=100,
    master_disk_size=100,
    zone="us-central1-a",
    subnetwork_uri="default",
    properties={
        "spark:spark.default.parallelism": str(
            NUM_WORKERS * VCPUS_PER_WORKER * PARALLELISM_MULTIPLIER
        )
    },
    enable_component_gateway=True,
).make()

sense_prefix = GoogleCloudStoragePrefixSensor(
    task_id="sense_data_prefix",
    bucket="7506-nyc-taxi",
    prefix="dataset/yellow/year={{ dag_run.logical_date.year }}/month={{ dag_run.logical_date.month }}",
    dag=dag,
)

create_dataproc_cluster = DataprocCreateClusterOperator(
    task_id="create_data_agg_dataproc_cluster",
    project_id=project_id,
    cluster_name=cluster_name,
    cluster_config=CLUSTER_CONFIG,
    region="us-central1",
    dag=dag,
)

submit_job = DataprocSubmitPySparkJobOperator(
    task_id="submit_aggregate_data_task",
    job_name="pyspark_aggregate_data_task",
    project_id=project_id,
    cluster_name=cluster_name,
    main="gs://7506-spark/jobs/aggregate_yellow_data.py",
    arguments=[
        "--bucket-name",
        "7506-nyc-taxi",
        "--year",
        "{{ dag_run.logical_date.year }}",
        "--month",
        "{{ dag_run.logical_date.month }}",
    ],
    region="us-central1",
    files=["gs://7506-spark/jars/gcs-connector-hadoop3-latest.jar"],
    dag=dag,
)

delete_dataproc_cluster = DataprocDeleteClusterOperator(
    task_id="delete_data_agg_dataproc_cluster",
    project_id=project_id,
    region="us-central1",
    cluster_name=cluster_name,
    trigger_rule=trigger_rule.TriggerRule.ALL_DONE,
    dag=dag,
)

sense_prefix >> create_dataproc_cluster >> submit_job >> delete_dataproc_cluster
