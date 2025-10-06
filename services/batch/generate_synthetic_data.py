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
    Product("Cold Brew Coffee", 3.75, 5.25),
    Product("Almond Croissant", 2.95, 4.10),
    Product("Organic Granola", 5.50, 7.25),
    Product("Matcha Latte", 4.10, 5.75),
    Product("Vegan Protein Bar", 2.25, 3.60),
    Product("Spinach Feta Wrap", 6.25, 8.10),
    Product("Artisan Sourdough", 4.20, 6.80),
    Product("Greek Yogurt Parfait", 3.95, 5.50),
    Product("Avocado Toast", 5.95, 8.45),
    Product("Fresh Pressed Juice", 4.50, 6.90),
    Product("Dark Chocolate Brownie", 2.50, 3.95),
    Product("Seasonal Salad", 7.10, 9.50),
    Product("Chia Seed Pudding", 3.40, 4.90),
    Product("Roasted Chicken Bowl", 8.20, 11.50),
    Product("Iced Caramel Latte", 3.90, 5.60),
]

STORES = [
    ("NYC-01", "Manhattan"),
    ("NYC-02", "Brooklyn"),
    ("SF-01", "Downtown"),
    ("SEA-01", "Capitol Hill"),
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
