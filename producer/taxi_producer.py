

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer


DEFAULT_BROKER = "localhost:9092"
DEFAULT_TOPIC = "taxi-trips"
DEFAULT_SOURCE_PATH = "data_stream/test_trips"
EVENT_COLUMNS = ["pickup_datetime", "start_lon", "start_lat"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish clean individual test trips to Kafka.")
    parser.add_argument("--broker", default=DEFAULT_BROKER, help="Kafka bootstrap server.")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Kafka topic to publish to.")
    parser.add_argument(
        "--source-path",
        default=DEFAULT_SOURCE_PATH,
        help="Parquet directory with clean individual test trips.",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.001,
        help="Seconds to wait between messages. Use 0 for maximum speed.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50000,
        help="Maximum messages to send. Use 0 to send all rows.",
    )
    parser.add_argument("--log-every", type=int, default=1000, help="Print progress every N messages.")
    return parser.parse_args()


def build_producer(broker: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
        linger_ms=20,
    )


def load_events(source_path: Path, limit: int) -> pd.DataFrame:
    if not source_path.exists():
        raise FileNotFoundError(
            f"No existe {source_path}. Genera antes el dataset con "
            "python producer/prepare_stream_test_data.py"
        )

    df = pd.read_parquet(source_path, columns=EVENT_COLUMNS)
    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
    df = df.dropna(subset=EVENT_COLUMNS).sort_values(["pickup_datetime", "start_lon", "start_lat"])

    if limit > 0:
        df = df.head(limit)

    return df


def row_to_message(row) -> dict:
    return {
        "pickup_datetime": str(row.pickup_datetime),
        "start_lon": float(row.start_lon),
        "start_lat": float(row.start_lat),
    }


def main() -> None:
    args = parse_args()
    source_path = Path(args.source_path)

    df = load_events(source_path, args.limit)
    if df.empty:
        raise ValueError("No hay viajes limpios para publicar.")

    producer = build_producer(args.broker)
    print(f"Conectado a Kafka en {args.broker}")
    print(f"Publicando en topic: {args.topic}")
    print(f"Fuente de eventos limpios: {source_path}")
    print(f"Filas individuales a enviar: {len(df):,}")

    started = time.time()
    total = 0

    for row in df.itertuples(index=False):
        message = row_to_message(row)
        zone_key = f"{round(message['start_lon'], 2)}:{round(message['start_lat'], 2)}"
        producer.send(args.topic, key=zone_key, value=message)
        total += 1

        if total % args.log_every == 0:
            elapsed = max(time.time() - started, 0.001)
            print(f"  Enviados: {total:,} | {total / elapsed:,.0f} msg/s")

        if args.sleep > 0:
            time.sleep(args.sleep)

    producer.flush()
    elapsed = max(time.time() - started, 0.001)
    print(f"\nFinalizado. Total: {total:,} viajes publicados en {elapsed:.1f}s.")


if __name__ == "__main__":
    main()
