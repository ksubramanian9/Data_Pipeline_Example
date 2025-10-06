from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import timezone
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import pyarrow.dataset as ds
from flask import Flask, jsonify, send_from_directory

STREAM_PARQUET_DIR = Path(
    os.environ.get("STREAM_PARQUET_DIR", "/opt/spark-data/output/streaming_product_revenue")
)
MAX_WINDOWS = int(os.environ.get("STREAM_WINDOWS_LIMIT", "96"))
MAX_SERIES = int(os.environ.get("STREAM_SERIES_LIMIT", "8"))

app = Flask(__name__, static_folder="static", static_url_path="/static")


@dataclass
class StreamPayload:
    status: str
    last_updated: Optional[str] = None
    summary: Optional[Dict[str, object]] = None
    timeline: Optional[List[Dict[str, object]]] = None
    leaderboard: Optional[List[Dict[str, object]]] = None
    window_health: Optional[List[Dict[str, object]]] = None
    raw_windows: Optional[int] = None

    def to_json(self) -> Dict[str, object]:
        payload: Dict[str, object] = {"status": self.status}
        if self.last_updated:
            payload["last_updated"] = self.last_updated
        if self.summary:
            payload["summary"] = self.summary
        if self.timeline is not None:
            payload["timeline"] = self.timeline
        if self.leaderboard is not None:
            payload["leaderboard"] = self.leaderboard
        if self.window_health is not None:
            payload["window_health"] = self.window_health
        if self.raw_windows is not None:
            payload["raw_windows"] = self.raw_windows
        return payload


def _format_ts(value: pd.Timestamp) -> str:
    if value.tzinfo is None:
        value = value.tz_localize(timezone.utc)
    else:
        value = value.tz_convert(timezone.utc)
    return value.isoformat().replace("+00:00", "Z")


def _load_streaming_dataframe() -> Optional[pd.DataFrame]:
    if not STREAM_PARQUET_DIR.exists():
        return None

    dataset = ds.dataset(str(STREAM_PARQUET_DIR), format="parquet")
    table = dataset.to_table()
    if table.num_rows == 0:
        return None

    df = table.to_pandas()
    required_columns = {"product", "revenue", "window_start", "window_end"}
    if not required_columns.issubset(df.columns):
        missing = sorted(required_columns - set(df.columns))
        raise ValueError(f"Parquet dataset missing expected columns: {missing}")

    df["revenue"] = df["revenue"].astype(float)
    df["window_start"] = pd.to_datetime(df["window_start"], utc=True)
    df["window_end"] = pd.to_datetime(df["window_end"], utc=True)
    df = df.sort_values(["window_start", "product"]).reset_index(drop=True)
    return df


def _build_payload(df: pd.DataFrame) -> StreamPayload:
    if df.empty:
        return StreamPayload(status="no_data")

    df = df.tail(MAX_WINDOWS * max(1, df["product"].nunique()))

    last_updated = _format_ts(df["window_end"].max())

    per_product = (
        df.groupby("product")["revenue"].sum().sort_values(ascending=False).reset_index()
    )
    top_products = per_product.head(MAX_SERIES)["product"].tolist()
    series: List[Dict[str, object]] = []
    for product in top_products:
        segment = df[df["product"] == product].sort_values("window_start")
        series.append(
            {
                "product": product,
                "total_revenue": round(float(segment["revenue"].sum()), 2),
                "points": [
                    {
                        "window_start": _format_ts(row.window_start),
                        "window_end": _format_ts(row.window_end),
                        "revenue": round(float(row.revenue), 2),
                    }
                    for row in segment.itertuples(index=False)
                ],
            }
        )

    latest_window_end = df["window_end"].max()
    latest_window = df[df["window_end"] == latest_window_end]
    leaderboard = (
        latest_window.groupby("product")["revenue"].sum().sort_values(ascending=False).reset_index()
    )
    leaderboard_records: List[Dict[str, object]] = [
        {
            "product": row.product,
            "window_end": _format_ts(latest_window_end),
            "revenue": round(float(row.revenue), 2),
        }
        for row in leaderboard.itertuples(index=False)
    ]

    window_health = (
        df.groupby(["window_start", "window_end"]).agg(
            total_revenue=("revenue", "sum"),
            product_count=("product", "nunique"),
        ).reset_index().sort_values("window_start", ascending=False)
    )
    window_health = window_health.head(MAX_WINDOWS)
    window_health_records: List[Dict[str, object]] = [
        {
            "window_start": _format_ts(row.window_start),
            "window_end": _format_ts(row.window_end),
            "total_revenue": round(float(row.total_revenue), 2),
            "product_count": int(row.product_count),
        }
        for row in window_health.itertuples(index=False)
    ]

    summary: Dict[str, object] = {
        "unique_products": int(df["product"].nunique()),
        "windows": int(df[["window_start", "window_end"]].drop_duplicates().shape[0]),
        "latest_window_end": _format_ts(latest_window_end),
    }

    return StreamPayload(
        status="ok",
        last_updated=last_updated,
        summary=summary,
        timeline=series,
        leaderboard=leaderboard_records,
        window_health=window_health_records,
        raw_windows=summary["windows"],
    )


@app.route("/")
def index() -> str:
    return send_from_directory("static", "index.html")


@app.route("/api/stream")
def api_stream() -> str:
    try:
        df = _load_streaming_dataframe()
        if df is None:
            payload = StreamPayload(status="no_data")
        else:
            payload = _build_payload(df)
    except Exception as exc:  # pragma: no cover - defensive catch for UI
        payload = StreamPayload(status="error", summary={"message": str(exc)})
    return jsonify(payload.to_json())


if __name__ == "__main__":
    port = int(os.environ.get("FLASK_RUN_PORT", "5100"))
    app.run(host="0.0.0.0", port=port, debug=False)
