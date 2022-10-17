import argparse
from datetime import datetime

import pyspark.sql.functions as F
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import SparkSession, SQLContext


def run_job(bucket: str, artifacts_bucket: str):
    spark = (
        SparkSession.builder.config(
            "spark.sql.sources.partitionOverwriteMode", "dynamic"
        )
        .appName("aggregate_yellow_data")
        .getOrCreate()
    )

    sc = spark.sparkContext
    sql_context = SQLContext(sc)
    df = sql_context.read.parquet(f"gs://{bucket}/analytical/yellow/")

    df = df.select("PULocationID", "hour", "qty", "dow")

    assembler = VectorAssembler(
        inputCols=["PULocationID", "hour", "dow"], outputCol="features"
    )
    transformed_data = assembler.transform(df)

    (training_data, test_data) = transformed_data.randomSplit([0.8, 0.2])

    rf = RandomForestRegressor(labelCol="qty", featuresCol="features", maxDepth=5)

    model = rf.fit(training_data)

    predictions = model.transform(test_data)

    evaluator = RegressionEvaluator(
        labelCol="qty", predictionCol="prediction", metricName="mae"
    )

    mae = evaluator.evaluate(predictions)
    print("Test MAE = ", mae)

    model.save(f"gs://{artifacts_bucket}/yellow/{datetime.now():%Y_%m_%d}")


def main():
    """Main entrypoint."""
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--bucket-name", type=str, default="7506-nyc-taxi", help="")
    parser.add_argument(
        "--artifacts-bucket-name", type=str, default="7506-spark", help=""
    )

    args = parser.parse_args()

    run_job(args.bucket_name, args.artifacts_bucket_name)


if __name__ == "__main__":
    main()
