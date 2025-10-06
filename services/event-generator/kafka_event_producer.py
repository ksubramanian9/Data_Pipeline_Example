"""Kafka event generator for retail sales demo.

This script replays CSV input files as JSON order events to a Kafka topic.
It is intentionally lightweight so it can run either directly on the host
or inside the `event-generator` service defined in ``docker-compose.yml``.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import random
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List

from kafka import KafkaProducer


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(line_buffering=True)

logger = logging.getLogger("retail.kafka.producer")


@dataclass
class ProducerConfig:
    """Runtime configuration derived from CLI arguments."""

    bootstrap_servers: str
    topic: str
    input_dir: Path
    rate_per_second: float
    shuffle: bool
    loop: bool


class EventProducer:
    """Replay CSV sales rows to Kafka as JSON order events."""

    def __init__(self, config: ProducerConfig) -> None:
        self._config = config
        self._producer = KafkaProducer(
            bootstrap_servers=config.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=100,
        )

    def _discover_csv_files(self) -> List[Path]:
        files = sorted(self._config.input_dir.glob("*.csv"))
        if not files:
            raise FileNotFoundError(
                f"No CSV files found under {self._config.input_dir}"
            )
        return files

    def _iter_rows(self, files: Iterable[Path]) -> Iterator[Dict[str, str]]:
        for path in files:
            logger.info("Loading %s", path)
            with path.open("r", newline="") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    if not any(row.values()):
                        continue
                    clean = {k.strip(): (v.strip() if isinstance(v, str) else v)
                             for k, v in row.items()}
                    yield clean

    def _normalise_row(self, row: Dict[str, str]) -> Dict[str, str]:
        payload = dict(row)
        # Provide a consistent timestamp field for the stream job.
        for candidate in ("order_ts", "order_time", "order_date", "timestamp"):
            if candidate in payload and payload[candidate]:
                payload["event_time"] = payload[candidate]
                break
        if "event_time" not in payload:
            payload["event_time"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        return payload

    def run(self) -> None:
        files = self._discover_csv_files()
        logger.info(
            "Starting producer with rate %.2f msg/s (shuffle=%s, loop=%s)",
            self._config.rate_per_second,
            self._config.shuffle,
            self._config.loop,
        )

        rows = list(self._iter_rows(files))
        if self._config.shuffle:
            random.shuffle(rows)

        delay = 1.0 / self._config.rate_per_second if self._config.rate_per_second > 0 else 0

        sent = 0
        try:
            while True:
                for row in rows:
                    payload = self._normalise_row(row)
                    self._producer.send(self._config.topic, payload)
                    sent += 1
                    if sent % 100 == 0:
                        logger.info("Published %d events", sent)
                    if delay > 0:
                        time.sleep(delay)
                if not self._config.loop:
                    break
                if self._config.shuffle:
                    random.shuffle(rows)
        finally:
            logger.info("Flushing producer (sent %d events)", sent)
            self._producer.flush()
            self._producer.close()


def parse_args(argv: List[str]) -> ProducerConfig:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        help="Kafka bootstrap server string",
    )
    parser.add_argument(
        "--topic",
        default=os.environ.get("KAFKA_TOPIC", "sales"),
        help="Kafka topic to publish to",
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path(os.environ.get("EVENT_INPUT_DIR", "/opt/spark-data/input")),
        help="Directory containing CSV files to replay",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=float(os.environ.get("EVENTS_PER_SECOND", "5")),
        help="Target events per second (<=0 for as fast as possible)",
    )
    parser.add_argument(
        "--shuffle",
        action="store_true",
        help="Randomise the order of events within the loaded dataset",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Continuously loop over the dataset instead of stopping after one pass",
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("LOG_LEVEL", "INFO"),
        help="Logging level",
    )

    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="[%(asctime)s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    config = ProducerConfig(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        input_dir=args.input_dir,
        rate_per_second=max(args.rate, 0.0),
        shuffle=args.shuffle,
        loop=args.loop,
    )
    return config


def main(argv: List[str] | None = None) -> None:
    config = parse_args(argv or sys.argv[1:])
    producer = EventProducer(config)
    producer.run()


if __name__ == "__main__":
    main()
