import os, glob
import pandas as pd
from flask import Flask, jsonify, send_from_directory

OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/opt/spark-data/output")
CSV_DIR = os.path.join(OUTPUT_DIR, "analysis_csv")

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
    # Ensure expected columns exist
    cols = [c.lower() for c in df.columns]
    df.columns = cols
    assert "order_date" in cols and "product" in cols and "total_amount" in cols

    # totals per day
    per_day = (
        df.groupby("order_date")["total_amount"]
          .sum().reset_index().sort_values("order_date")
    )
    # top products overall
    top_products = (
        df.groupby("product")["total_amount"]
          .sum().reset_index().sort_values("total_amount", ascending=False).head(10)
    )
    # sample table
    sample_rows = df.sort_values(["order_date","product"]).head(50)

    return jsonify({
        "status": "ok",
        "daily": per_day.to_dict(orient="records"),
        "top_products": top_products.to_dict(orient="records"),
        "sample": sample_rows.to_dict(orient="records"),
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
