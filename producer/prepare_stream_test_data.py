"""
Prepare clean individual test trips for the Kafka streaming demo.

This script takes the noisy raw taxi parquet and applies the same core cleaning
decisions used in the batch pipeline. The result is NOT aggregated: each row is
one clean taxi pickup event. That is the right input for Kafka, because Spark
Streaming should be the component that groups events into demand windows.

Run from the repository root:
    python producer/prepare_stream_test_data.py
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


DEFAULT_RAW_PATH = "data/*.parquet"
DEFAULT_OUTPUT_PATH = "data_stream/test_trips"
DEFAULT_TEST_FROM = "2009-01-27 00:00:00"
DEFAULT_TEST_TO = "2009-02-01 00:00:00"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create clean individual test trips for streaming.")
    parser.add_argument("--raw-path", default=DEFAULT_RAW_PATH, help="Raw parquet glob.")
    parser.add_argument("--output-path", default=DEFAULT_OUTPUT_PATH, help="Output parquet directory.")
    parser.add_argument("--test-from", default=DEFAULT_TEST_FROM, help="Inclusive pickup start timestamp.")
    parser.add_argument("--test-to", default=DEFAULT_TEST_TO, help="Exclusive pickup end timestamp.")
    parser.add_argument("--partitions", type=int, default=8, help="Output partitions.")
    return parser.parse_args()


def create_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("prepare-stream-test-trips")
        .master("local[*]")
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def main() -> None:
    args = parse_args()
    Path(args.output_path).parent.mkdir(parents=True, exist_ok=True)

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(args.raw_path)
    distance_col = "Trip_Distance" if "Trip_Distance" in df.columns else "Trip_distance"

    pickup_ts = F.col("Trip_Pickup_DateTime").cast("timestamp")
    dropoff_ts = F.col("Trip_Dropoff_DateTime").cast("timestamp")

    clean_trips = (
        df.withColumn("pickup_datetime", pickup_ts)
        .withColumn("dropoff_datetime", dropoff_ts)
        .withColumn("start_lon", F.col("Start_Lon").cast("double"))
        .withColumn("start_lat", F.col("Start_Lat").cast("double"))
        .withColumn(
            "trip_duration_minutes",
            (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60.0,
        )
        .filter(F.col(distance_col) > 0.1)
        .filter(F.col("dropoff_datetime") > F.col("pickup_datetime"))
        .filter(
            (F.col("start_lon") >= -74.3)
            & (F.col("start_lon") <= -73.7)
            & (F.col("start_lat") >= 40.5)
            & (F.col("start_lat") <= 40.9)
        )
        .filter(F.col("trip_duration_minutes") <= 180)
        .filter(F.col("pickup_datetime") >= F.lit(args.test_from).cast("timestamp"))
        .filter(F.col("pickup_datetime") < F.lit(args.test_to).cast("timestamp"))
        .select("pickup_datetime", "start_lon", "start_lat")
        .orderBy("pickup_datetime", "start_lon", "start_lat")
    )

    count = clean_trips.count()
    (
        clean_trips.repartition(args.partitions)
        .write.mode("overwrite")
        .parquet(args.output_path)
    )

    print(f"Viajes individuales limpios guardados en: {args.output_path}")
    print(f"Periodo test: [{args.test_from}, {args.test_to})")
    print(f"Filas guardadas: {count:,}")

    spark.stop()


if __name__ == "__main__":
    main()
