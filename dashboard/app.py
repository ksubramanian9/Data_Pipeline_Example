import glob
import os
from typing import List

import pandas as pd
from flask import Flask, jsonify, send_from_directory

from schema_metadata import load_schema_metadata

OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/opt/spark-data/output")
CSV_DIR = os.path.join(OUTPUT_DIR, "analysis_csv")
SCHEMA_CONFIG_PATH = os.environ.get(
    "SCHEMA_CONFIG_PATH", "/opt/services/batch/schemas/card_transactions.json"
)

METADATA = load_schema_metadata(SCHEMA_CONFIG_PATH)

app = Flask(__name__, static_folder="static", static_url_path="/static")

def latest_csv_path():
    path = CSV_DIR
    if not os.path.isdir(path):
        return None
    files = sorted(glob.glob(os.path.join(path, "*.csv")), key=os.path.getmtime, reverse=True)
    return files[0] if files else None

@app.route("/")
def index():
    return send_from_directory("static", "index.html")

@app.route("/api/daily")
def api_daily():
    p = latest_csv_path()
    if not p or not os.path.exists(p):
        return jsonify({"status": "no_data", "message": "No analysis CSV found yet."})
    df = pd.read_csv(p)
    cols = [c.lower() for c in df.columns]
    df.columns = cols

    date_column = METADATA.date_column
    value_column = METADATA.value_column

    if date_column not in df.columns:
        return jsonify(
            {
                "status": "error",
                "message": f"Missing expected date column '{date_column}' in analysis CSV",
            }
        )

    if value_column not in df.columns:
        fallback = "record_count"
        if fallback in df.columns:
            value_column = fallback
        else:
            return jsonify(
                {
                    "status": "error",
                    "message": "Missing metric columns in analysis CSV",
                }
            )

    for dimension in METADATA.dimensions:
        if dimension not in df.columns:
            df[dimension] = "UNKNOWN"

    df[date_column] = pd.to_datetime(df[date_column]).dt.date

    per_day = (
        df.groupby(date_column)[value_column]
        .sum()
        .reset_index()
        .sort_values(date_column)
    )

    segment_dims: List[str] = METADATA.dimensions[:2]
    if segment_dims:
        segment_label = df[segment_dims[0]].fillna("UNKNOWN")
        for dim in segment_dims[1:]:
            segment_label = segment_label + " · " + df[dim].fillna("UNKNOWN")
        df["segment"] = segment_label
        top_segments = (
            df.groupby("segment")[value_column]
            .sum()
            .reset_index()
            .sort_values(value_column, ascending=False)
            .head(10)
        )
    else:
        top_segments = pd.DataFrame(columns=["segment", value_column])

    if METADATA.dimensions:
        dim = METADATA.dimensions[0]
        top_categories = (
            df.groupby(dim)[value_column]
            .sum()
            .reset_index()
            .sort_values(value_column, ascending=False)
            .head(10)
        )
    else:
        top_categories = pd.DataFrame(columns=[value_column])

    sort_columns = [date_column] + METADATA.dimensions
    ascending_flags = [False] + [True] * len(METADATA.dimensions)
    table_columns = list(dict.fromkeys(sort_columns + [value_column, "record_count"]))
    existing_columns = [col for col in table_columns if col in df.columns]
    sample_rows = df.sort_values(sort_columns, ascending=ascending_flags).head(50)

    meta_payload = {
        "dataset_name": METADATA.dataset_name,
        "date_column": date_column,
        "value_column": value_column,
        "dimensions": METADATA.dimensions,
        "numeric_fields": METADATA.numeric_fields,
        "boolean_fields": METADATA.boolean_fields,
        "currency": METADATA.currency,
        "primary_metric": METADATA.primary_metric,
        "table_columns": existing_columns,
    }

    return jsonify(
        {
            "status": "ok",
            "meta": meta_payload,
            "daily": per_day.to_dict(orient="records"),
            "top_segments": top_segments.to_dict(orient="records"),
            "top_categories": top_categories.to_dict(orient="records"),
            "sample": sample_rows[existing_columns].to_dict(orient="records"),
        }
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
