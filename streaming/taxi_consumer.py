

from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


KAFKA_BROKER = "localhost:9092"
TOPIC = "taxi-trips"
MODEL_PATH = "models/best_demand_model"
OUTPUT_PATH = "streaming_output"
CHECKPOINT_DIR = "streaming_checkpoint"
GRID_SIZE = 0.01
WINDOW_SIZE = "15 minutes"
WATERMARK = "2 hours"
PROCESSING_TRIGGER = "10 seconds"
DEFAULT_PREDICT_FROM = "2009-01-27 00:45:00"

# key: (zone_lon, zone_lat) -> {window_start: trip_count}
history_by_zone: dict[tuple[float, float], dict[object, float]] = {}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume clean individual taxi events and predict demand.")
    parser.add_argument(
        "--starting-offsets",
        choices=["latest", "earliest"],
        default="latest",
        help=(
            "latest reads only rows produced after the consumer starts; "
            "earliest also reads rows already stored in the topic. "
            "If a checkpoint exists, Spark resumes from the checkpoint."
        ),
    )
    parser.add_argument("--output-path", default=OUTPUT_PATH, help="Parquet output directory.")
    parser.add_argument("--checkpoint-dir", default=CHECKPOINT_DIR, help="Spark checkpoint directory.")
    parser.add_argument("--model-path", default=MODEL_PATH, help="Saved Spark PipelineModel path.")
    parser.add_argument(
        "--predict-from",
        default=DEFAULT_PREDICT_FROM,
        help="First window_start allowed for prediction after lags are available in the test stream.",
    )
    return parser.parse_args()


def create_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("nyc-taxi-demand-streaming")
        .master("local[*]")
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def zone_key(zone_lon: float, zone_lat: float) -> tuple[float, float]:
    return round(float(zone_lon), 4), round(float(zone_lat), 4)


def lag_value(hist: dict, current_window, steps: int) -> float | None:
    target_window = current_window - timedelta(minutes=15 * steps)
    value = hist.get(target_window)
    return None if value is None else float(value)


def add_model_features(rows, spark: SparkSession, predict_from: datetime):
    enriched_rows = []
    skipped_without_lags = 0
    skipped_before_start = 0

    for row in rows:
        current_window = row["window_start"]
        trip_count = float(row["trip_count"])
        key = zone_key(row["zone_lon"], row["zone_lat"])
        hist = history_by_zone.setdefault(key, {})

        lag_1 = lag_value(hist, current_window, 1)
        lag_2 = lag_value(hist, current_window, 2)

        if current_window < predict_from:
            skipped_before_start += 1
        elif lag_1 is None or lag_2 is None:
            skipped_without_lags += 1
        else:
            enriched_rows.append(
                {
                    "window_start": current_window,
                    "window_end": row["window_end"],
                    "zone_lon": key[0],
                    "zone_lat": key[1],
                    "trip_count": trip_count,
                    "hour": int(row["hour"]),
                    "dayofweek": int(row["dayofweek"]),
                    "is_weekend": int(row["is_weekend"]),
                    "is_rush_hour": int(row["is_rush_hour"]),
                    "is_late_night": int(row["is_late_night"]),
                    "lag_1": lag_1,
                    "lag_2": lag_2,
                }
            )

        hist[current_window] = trip_count

        if len(hist) > 240:
            keep_keys = sorted(hist.keys())[-120:]
            history_by_zone[key] = {k: hist[k] for k in keep_keys}

    if not enriched_rows:
        return None, skipped_before_start, skipped_without_lags

    return spark.createDataFrame(enriched_rows), skipped_before_start, skipped_without_lags


def main() -> None:
    args = parse_args()
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    model_path = Path(args.model_path)
    if not model_path.exists():
        raise FileNotFoundError(f"Model not found: {args.model_path}")

    print(f"Cargando modelo desde {args.model_path}...")
    model = PipelineModel.load(args.model_path)
    print("Modelo cargado.\n")

    predict_from = datetime.fromisoformat(args.predict_from)

    schema = StructType(
        [
            StructField("pickup_datetime", StringType()),
            StructField("start_lon", DoubleType()),
            StructField("start_lat", DoubleType()),
        ]
    )

    def process_batch(aggregated_batch, epoch_id: int) -> None:
        if aggregated_batch.rdd.isEmpty():
            print(f"[Batch {epoch_id}] Sin mensajes.")
            return

        rows = aggregated_batch.orderBy("window_start", "zone_lon", "zone_lat").collect()
        features_df, skipped_before_start, skipped_without_lags = add_model_features(rows, spark, predict_from)
        if features_df is None:
            print(
                f"[Batch {epoch_id}] Esperando lags suficientes "
                f"(antes de inicio: {skipped_before_start:,}, sin lags: {skipped_without_lags:,})."
            )
            return

        predictions = model.transform(features_df)
        result = (
            predictions.select(
                "window_start",
                "window_end",
                "zone_lon",
                "zone_lat",
                "trip_count",
                "prediction",
            )
            .withColumn("error", F.col("prediction") - F.col("trip_count"))
            .withColumn("abs_error", F.abs(F.col("error")))
        )

        total_zones = result.count()
        mae = result.agg(F.avg("abs_error").alias("mae")).first()["mae"]
        print(
            f"\n[Batch {epoch_id}] Ventanas/zona predichas: {total_zones:,} | MAE batch: {mae:.4f} "
            f"| omitidas antes de inicio: {skipped_before_start:,} | omitidas sin lags: {skipped_without_lags:,}"
        )
        result.orderBy(F.col("prediction").desc()).show(10, truncate=False)

        result.write.mode("append").parquet(args.output_path)

    events_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TOPIC)
        .option("startingOffsets", args.starting_offsets)
        .load()
        .select(F.from_json(F.col("value").cast("string"), schema).alias("payload"))
        .select("payload.*")
        .withColumn("pickup_datetime", F.to_timestamp("pickup_datetime"))
        .dropna(subset=["pickup_datetime", "start_lon", "start_lat"])
        .withWatermark("pickup_datetime", WATERMARK)
        .withColumn("zone_lon", F.floor(F.col("start_lon") / GRID_SIZE) * GRID_SIZE)
        .withColumn("zone_lat", F.floor(F.col("start_lat") / GRID_SIZE) * GRID_SIZE)
    )

    aggregated_stream = (
        events_stream.groupBy(
            F.window(F.col("pickup_datetime"), WINDOW_SIZE),
            "zone_lon",
            "zone_lat",
        )
        .count()
        .withColumnRenamed("count", "trip_count")
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .withColumn("hour", F.hour("window_start"))
        .withColumn("dayofweek", F.dayofweek("window_start"))
        .withColumn("is_weekend", F.col("dayofweek").isin([1, 7]).cast("int"))
        .withColumn("is_rush_hour", F.col("hour").isin([7, 8, 9, 17, 18, 19]).cast("int"))
        .withColumn("is_late_night", F.col("hour").isin([0, 1, 2, 3, 22, 23]).cast("int"))
        .drop("window")
    )

    query = (
        aggregated_stream.writeStream.outputMode("update")
        .foreachBatch(process_batch)
        .trigger(processingTime=PROCESSING_TRIGGER)
        .option("checkpointLocation", args.checkpoint_dir)
        .start()
    )

    print("Stream de demanda arrancado.")
    print(f"Kafka: {KAFKA_BROKER} | topic: {TOPIC}")
    print(f"startingOffsets: {args.starting_offsets}")
    print(f"Primera ventana predicha desde: {args.predict_from}")
    print("Fuente esperada: viajes individuales limpios de data_stream/test_trips.")
    print("Ctrl+C para parar.\n")

    query.awaitTermination()


if __name__ == "__main__":
    main()
