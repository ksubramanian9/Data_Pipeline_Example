"""Utility to create synthetic retail transaction CSV files for the batch pipeline.

The generator creates one CSV file per day with a realistic product catalogue so the
Spark batch job has varied data to process. The script intentionally keeps the number
of transactions modest so the dataset stays lightweight for demos while still
exercising the ETL logic (amount calculation, aggregation, etc.).
"""

from __future__ import annotations

import argparse
import csv
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, List


@dataclass(frozen=True)
class Product:
    name: str
    min_price: float
    max_price: float

    def sample_price(self) -> float:
        price = random.uniform(self.min_price, self.max_price)
        return round(price, 2)


CATALOGUE: List[Product] = [
    Product("Masala Dosa", 80.0, 140.0),
    Product("Paneer Tikka Wrap", 120.0, 180.0),
    Product("Vegetable Biryani", 150.0, 220.0),
    Product("Idli Sambar Combo", 60.0, 110.0),
    Product("Chole Bhature", 90.0, 150.0),
    Product("Pav Bhaji", 80.0, 130.0),
    Product("Vegetable Pulao", 110.0, 170.0),
    Product("Paneer Butter Masala", 180.0, 260.0),
    Product("Mango Lassi", 70.0, 120.0),
    Product("Filter Coffee", 40.0, 80.0),
    Product("Masala Chai", 25.0, 60.0),
    Product("Gulab Jamun Pack", 90.0, 140.0),
    Product("Vegetable Sandwich", 50.0, 90.0),
    Product("Ragi Millet Bowl", 130.0, 190.0),
    Product("Pani Puri Kit", 60.0, 110.0),
]

STORES = [
    ("BLR-01", "Bengaluru - Indiranagar"),
    ("BLR-02", "Bengaluru - Koramangala"),
    ("DEL-01", "Delhi - Connaught Place"),
    ("DEL-02", "Delhi - Saket"),
    ("MUM-01", "Mumbai - Bandra"),
    ("MUM-02", "Mumbai - Powai"),
    ("CHE-01", "Chennai - T Nagar"),
    ("CHE-02", "Chennai - Velachery"),
    ("HYD-01", "Hyderabad - Banjara Hills"),
    ("HYD-02", "Hyderabad - Gachibowli"),
    ("KOL-01", "Kolkata - Park Street"),
    ("KOL-02", "Kolkata - Salt Lake"),
    ("PUN-01", "Pune - Hinjewadi"),
    ("PUN-02", "Pune - Kalyani Nagar"),
    ("AHM-01", "Ahmedabad - Vastrapur"),
    ("AHM-02", "Ahmedabad - Prahlad Nagar"),
    ("JAI-01", "Jaipur - C Scheme"),
    ("JAI-02", "Jaipur - Malviya Nagar"),
    ("LKO-01", "Lucknow - Hazratganj"),
    ("LKO-02", "Lucknow - Gomti Nagar"),
    ("CHD-01", "Chandigarh - Sector 17"),
    ("CHD-02", "Chandigarh - Elante"),
    ("KOCHI-01", "Kochi - Marine Drive"),
    ("KOCHI-02", "Kochi - Kakkanad"),
    ("SUR-01", "Surat - Adajan"),
    ("SUR-02", "Surat - Vesu"),
    ("IND-01", "Indore - Vijay Nagar"),
    ("IND-02", "Indore - Old Palasia"),
    ("GOA-01", "Goa - Panaji"),
    ("GOA-02", "Goa - Margao"),
]


def _ensure_output_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _clear_existing_files(path: Path, *, extension: str = ".csv") -> None:
    for file in path.glob(f"*{extension}"):
        file.unlink()


def daterange(start: date, days: int) -> Iterable[date]:
    for offset in range(days):
        yield start + timedelta(days=offset)


def generate_day_file(day: date, output_dir: Path, transactions: int, stores: List[tuple[str, str]]) -> Path:
    rows = []
    for seq in range(transactions):
        product = random.choice(CATALOGUE)
        quantity = random.randint(1, 5)
        unit_price = product.sample_price()

        # introduce occasional promotional discounts
        discount_factor = random.choice([1.0, 1.0, 1.0, 0.9, 0.95])
        unit_price = round(unit_price * discount_factor, 2)
        total = round(unit_price * quantity, 2)

        store_id, store_city = random.choice(stores)
        order_id = f"{day.strftime('%Y%m%d')}-{seq + 1:04d}"

        rows.append({
            "order_id": order_id,
            "order_date": day.isoformat(),
            "store_id": store_id,
            "store_city": store_city,
            "product": product.name,
            "quantity": quantity,
            "unit_price": f"{unit_price:.2f}",
            "amount": f"{total:.2f}",
        })

    output_path = output_dir / f"retail_{day.isoformat()}.csv"
    with output_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    return output_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic retail CSV inputs")
    parser.add_argument("--output", default="/opt/spark-data/input", help="Directory for generated CSV files")
    parser.add_argument("--days", type=int, default=30, help="Number of daily files to generate")
    parser.add_argument("--transactions-per-day", type=int, default=48, help="Transactions per generated day")
    parser.add_argument("--seed", type=int, default=2024, help="Random seed for reproducibility")
    parser.add_argument(
        "--start-date",
        help="Start date (YYYY-MM-DD). Defaults to days ending today.",
    )
    parser.add_argument("--keep-existing", action="store_true", help="Do not delete pre-existing CSV files in the output directory")
    return parser.parse_args()


def determine_start_date(days: int, explicit: str | None) -> date:
    if explicit:
        try:
            return datetime.strptime(explicit, "%Y-%m-%d").date()
        except ValueError as exc:
            raise SystemExit(f"Invalid --start-date '{explicit}': {exc}")

    today = date.today()
    return today - timedelta(days=days - 1)


def main() -> None:
    args = parse_args()
    random.seed(args.seed)

    output_dir = Path(args.output)
    _ensure_output_dir(output_dir)

    if not args.keep_existing:
        _clear_existing_files(output_dir)

    start_day = determine_start_date(args.days, args.start_date)

    generated_files = []
    for day in daterange(start_day, args.days):
        path = generate_day_file(day, output_dir, args.transactions_per_day, STORES)
        generated_files.append(path)

    print(f"Generated {len(generated_files)} file(s) under {output_dir}")
    for path in generated_files:
        print(f" - {path.name}")


if __name__ == "__main__":
    main()
