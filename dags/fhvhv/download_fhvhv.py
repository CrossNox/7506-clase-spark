import enum
import tempfile
from datetime import datetime

import requests
import youconfigme as ycm
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage
from tqdm import tqdm

DEFAULT_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset}_tripdata_{year}-{month:02}.parquet"

cfg = ycm.Config(from_items={})


class Dataset(str, enum.Enum):
    YELLOW = "yellow"
    GREEN = "green"
    FHV = "fhv"
    FHVHV = "fhvhv"


def download_dataset(
    dataset: Dataset,
    year: int,
    month: int,
    bucket: str = "7506-nyc-taxi",
    prefix: str = "dataset",
    chunk_size: int = 4096,
    file_format: str = "parquet",
):
    url = cfg.dataset.url(default=DEFAULT_URL).format(
        year=year, month=month, dataset=dataset, file_format=file_format
    )
    res = requests.get(url, stream=True)
    res.raise_for_status()
    with tqdm(
        desc="Downloading file",
        unit="B",
        total=int(res.headers["Content-Length"]),
        unit_scale=True,
        unit_divisor=chunk_size,
    ) as bar, tempfile.NamedTemporaryFile(mode="wb",) as outfile:
        for chunk in res.iter_content(chunk_size):
            outfile.write(chunk)
            bar.update(len(chunk))
        outfile.flush()

        client = storage.Client()
        client.bucket(bucket).blob(
            f"{prefix}/{dataset}/year={year}/month={month}/{dataset}.{file_format}"
        ).upload_from_filename(outfile.name)


default_args = {
    "owner": "7506",
    "start_date": datetime(2019, 1, 2),
    "retries": 1,
    "depends_on_past": False,
}


dag = DAG(
    "download_fhvhv_data",
    description="Monthly fhvhv data download",
    default_args=default_args,
    schedule_interval="@monthly",
    max_active_runs=4,
    catchup=True,
    render_template_as_native_obj=True,
    is_paused_upon_creation=True
)


def check_file_available(dataset, year, month, file_format):
    url = cfg.dataset.url(default=DEFAULT_URL).format(
        year=year, month=month, dataset=dataset, file_format=file_format
    )
    r = requests.get(url, stream=True)
    r.raise_for_status()


with dag:
    sense_fhvhv_parquet = PythonOperator(
        task_id="sense_fhvhv_parquet",
        python_callable=check_file_available,
        op_kwargs={
            "dataset": "fhvhv",
            "year": "{{ logical_date.subtract(months=2).year }}",
            "month": "{{ logical_date.subtract(months=2).month }}",
            "file_format": "parquet",
        },
    )

    download_fhvhv_parquet = PythonOperator(
        task_id="download_fhvhv_parquet",
        python_callable=download_dataset,
        op_kwargs={
            "dataset": "fhvhv",
            "year": "{{ logical_date.subtract(months=2).year }}",
            "month": "{{ logical_date.subtract(months=2).month }}",
            "file_format": "parquet",
        },
    )

    sense_fhvhv_parquet >> download_fhvhv_parquet
