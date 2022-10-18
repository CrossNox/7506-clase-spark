import argparse

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, SQLContext


def run_job(bucket: str, year: int, month: int):
    spark = (
        SparkSession.builder.config(
            "spark.sql.sources.partitionOverwriteMode", "dynamic"
        )
        .appName("aggregate_yellow_data")
        .getOrCreate()
    )

    sc = spark.sparkContext
    sql_context = SQLContext(sc)
    df = sql_context.read.parquet(
        f"gs://{bucket}/dataset/yellow/year={year}/month={month}/"
    )

    df = df.withColumn(
        "tpep_pickup_datetime",
        F.to_timestamp(df["tpep_pickup_datetime"], "yyyy-MM-dd HH:mm:ss"),
    )
    df = (
        df.withColumn("dow", F.dayofweek(df.tpep_pickup_datetime))
        .withColumn("year", F.year(df.tpep_pickup_datetime))
        .withColumn("month", F.month(df.tpep_pickup_datetime))
        .withColumn("day", F.dayofmonth(df.tpep_pickup_datetime))
        .withColumn("hour", F.hour(df.tpep_pickup_datetime))
        .withColumn("tip_perc", df.tip_amount / df.fare_amount)
    )

    df = df.groupBy(
        ["year", "month", "day", "hour", "dow", "PULocationID", "DOLocationID"]
    ).agg(
        F.count("tpep_dropoff_datetime").alias("qty"),
        F.avg("passenger_count").alias("avg_passenger_count"),
        F.avg("trip_distance").alias("avg_trip_distance"),
        F.avg("tip_perc").alias("avg_tip_perc"),
    )
    
    df.write.mode("overwrite").partitionBy("year", "month").parquet(
        "gs://7506-nyc-taxi/analytical/yellow/"
    )


def main():
    """Main entrypoint."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--bucket-name", type=str, default="7506-nyc-taxi", help=":(")
    parser.add_argument("--year", type=int, help="Year of data to process")
    parser.add_argument("--month", type=int, help="Month of data to process")

    args = parser.parse_args()

    run_job(args.bucket_name, args.year, args.month)


if __name__ == "__main__":
    main()
