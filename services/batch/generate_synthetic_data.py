#!/usr/bin/env python3
"""Schema-driven synthetic data generator for the batch pipeline.

This refactored utility consumes a JSON configuration that declares the
fields, distributions, and global timing constraints for the dataset. The
script keeps runtime dependencies minimal so it can run inside the existing
Docker image that previously executed the hard-coded retail generator.
"""

from __future__ import annotations

import argparse
import gzip
import json
import random
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Configurable synthetic dataset generator"
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to the JSON schema describing the dataset",
    )
    parser.add_argument(
        "--out",
        dest="out_file",
        help="Explicit output file path (overrides --output-dir/--output-name)",
    )
    parser.add_argument(
        "--output-dir",
        default=".",
        help="Directory to place the generated file when --out is not supplied",
    )
    parser.add_argument(
        "--output-name",
        help="Base file name without extension (defaults to 'synthetic_events')",
    )
    parser.add_argument(
        "--n",
        type=int,
        help="Override the record count declared in the config",
    )
    parser.add_argument(
        "--format",
        choices=["csv", "jsonl"],
        help="Override the output format from the config",
    )
    parser.add_argument(
        "--seed",
        type=int,
        help="Random seed for reproducibility (overrides config.seed)",
    )
    parser.add_argument(
        "--gzip",
        action="store_true",
        help="Write gzip-compressed output regardless of the config",
    )
    parser.add_argument(
        "--clear-output",
        action="store_true",
        help="Delete existing files that match the chosen output format before writing",
    )
    return parser.parse_args()


def weighted_choice(choices: Iterable[Tuple[Any, float]]) -> Any:
    values, weights = zip(*choices)
    total = float(sum(weights)) if weights else 0.0
    if total <= 0.0:
        return random.choice(values)
    r = random.uniform(0.0, total)
    upto = 0.0
    for value, weight in choices:
        upto += weight
        if upto >= r:
            return value
    return values[-1]


def pick_category(field_cfg: Dict[str, Any], context: Dict[str, Any]) -> Any:
    if "choices" in field_cfg:
        parsed: List[Tuple[Any, float]] = []
        for item in field_cfg["choices"]:
            if isinstance(item, dict):
                parsed.append((item["value"], float(item.get("weight", 1.0))))
            else:
                parsed.append((item, 1.0))
        return weighted_choice(parsed)

    conditional = field_cfg.get("conditional_on")
    if conditional:
        ref_value = context.get(conditional)
        table = field_cfg.get("conditional_table", {})
        bucket = (
            table.get(str(ref_value))
            or table.get(ref_value)
            or table.get("_default")
        )
        if not bucket:
            return "NA"
        parsed: List[Tuple[Any, float]] = []
        for item in bucket:
            if isinstance(item, dict):
                parsed.append((item["value"], float(item.get("weight", 1.0))))
            else:
                parsed.append((item, 1.0))
        return weighted_choice(parsed)

    return "NA"


def gen_string(field_cfg: Dict[str, Any]) -> str:
    pattern = field_cfg.get("pattern", "STR-{0000-9999}")
    out: List[str] = []
    i = 0
    while i < len(pattern):
        ch = pattern[i]
        if ch == "{" and "}" in pattern[i:]:
            j = pattern.index("}", i + 1)
            token = pattern[i + 1 : j]
            if token == "A-Z":
                out.append(chr(random.randint(65, 90)))
            elif token == "a-z":
                out.append(chr(random.randint(97, 122)))
            elif "-" in token and token.replace("-", "").isdigit():
                lo, hi = token.split("-")
                number = random.randint(int(lo), int(hi))
                width = max(len(lo), len(hi))
                out.append(str(number).zfill(width))
            else:
                out.append(token)
            i = j + 1
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def gen_int(field_cfg: Dict[str, Any]) -> int:
    lo = int(field_cfg.get("min", 0))
    hi = int(field_cfg.get("max", 100))
    skew = float(field_cfg.get("skew", 1.0))
    if skew == 1.0:
        return random.randint(lo, hi)
    u = random.random()
    value = int(lo + (hi - lo) * (u**skew))
    return max(lo, min(hi, value))


def gen_float(field_cfg: Dict[str, Any]) -> float:
    dist = field_cfg.get("distribution", "uniform")
    if dist == "uniform":
        lo = float(field_cfg.get("min", 0.0))
        hi = float(field_cfg.get("max", 1.0))
        return random.uniform(lo, hi)
    if dist == "normal":
        mu = float(field_cfg.get("mean", 0.0))
        sigma = float(field_cfg.get("stddev", 1.0))
        return random.gauss(mu, sigma)
    if dist == "lognormal":
        mu = float(field_cfg.get("mean", 0.0))
        sigma = float(field_cfg.get("stddev", 1.0))
        return random.lognormvariate(mu, sigma)
    if dist == "exponential":
        lam = float(field_cfg.get("lambda", 1.0))
        return random.expovariate(lam)
    # fallback to uniform
    lo = float(field_cfg.get("min", 0.0))
    hi = float(field_cfg.get("max", 1.0))
    return random.uniform(lo, hi)


def gen_bool(field_cfg: Dict[str, Any]) -> bool:
    p_true = float(field_cfg.get("p_true", 0.5))
    return random.random() < p_true


def gen_geo(field_cfg: Dict[str, Any]) -> Tuple[float, float]:
    bbox = field_cfg.get("bbox", [12.9, 77.5, 13.0, 77.7])
    lat = random.uniform(bbox[0], bbox[2])
    lon = random.uniform(bbox[1], bbox[3])
    precision = int(field_cfg.get("precision", 6))
    return round(lat, precision), round(lon, precision)


def seasonality_weight(ts: datetime, cfg: Dict[str, Any]) -> float:
    weight = 1.0
    hod = ts.hour
    dow = ts.weekday()
    hod_cfg = cfg.get("hour_of_day", {})
    if hod_cfg:
        if str(hod) in hod_cfg:
            weight *= float(hod_cfg[str(hod)])
        elif "_default" in hod_cfg:
            weight *= float(hod_cfg["_default"])
    dow_cfg = cfg.get("day_of_week", {})
    if dow_cfg:
        if str(dow) in dow_cfg:
            weight *= float(dow_cfg[str(dow)])
        elif "_default" in dow_cfg:
            weight *= float(dow_cfg["_default"])
    return weight


def gen_datetime(
    cfg: Dict[str, Any], index: int, total: int, global_time: Dict[str, str]
) -> datetime:
    start = datetime.fromisoformat(global_time["start"])
    end = datetime.fromisoformat(global_time["end"])
    mode = cfg.get("mode", "uniform")
    if mode == "ramp":
        t = index / max(1, total - 1)
        if cfg.get("direction", "up") == "down":
            t = 1.0 - t
        delta = (end - start) * t
        return start + delta
    if mode == "seasonality":
        profile = cfg.get("profile", {})
        for _ in range(1000):
            u = random.random()
            candidate = start + (end - start) * u
            weight = seasonality_weight(candidate, profile)
            if random.random() < min(1.0, weight):
                return candidate
        u = random.random()
        return start + (end - start) * u
    u = random.random()
    return start + (end - start) * u


def gen_field(
    field_cfg: Dict[str, Any],
    context: Dict[str, Any],
    index: int,
    total: int,
    global_time: Dict[str, str],
) -> Any:
    if random.random() < float(field_cfg.get("null_prob", 0.0)):
        return None
    ftype = field_cfg["type"]
    if ftype == "uuid":
        return str(uuid.uuid4())
    if ftype == "id_sequence":
        start = int(field_cfg.get("start", 1))
        step = int(field_cfg.get("step", 1))
        return start + index * step
    if ftype == "int":
        return gen_int(field_cfg)
    if ftype == "float":
        return gen_float(field_cfg)
    if ftype == "bool":
        return gen_bool(field_cfg)
    if ftype == "category":
        return pick_category(field_cfg, context)
    if ftype == "string":
        return gen_string(field_cfg)
    if ftype == "datetime":
        dt = gen_datetime(field_cfg, index, total, global_time)
        fmt = field_cfg.get("format", "iso")
        if fmt == "epoch_ms":
            return int(dt.timestamp() * 1000)
        if fmt == "epoch_s":
            return int(dt.timestamp())
        if fmt == "iso":
            return dt.isoformat()
        return dt.strftime(fmt)
    if ftype == "geo":
        lat, lon = gen_geo(field_cfg)
        if field_cfg.get("as_object", True):
            return {"lat": lat, "lon": lon}
        return f"{lat},{lon}"
    if ftype == "derived_concat":
        parts = field_cfg.get("parts", [])
        sep = field_cfg.get("sep", "-")
        values = [str(context.get(p, "")) for p in parts]
        return sep.join(values)
    if ftype == "derived_map":
        src = field_cfg.get("from_field")
        mapping = field_cfg.get("map", {})
        default = field_cfg.get("default")
        key = context.get(src)
        return mapping.get(str(key), mapping.get(key, default))
    return None


def open_out(path: Path, use_gzip: bool):
    if use_gzip or str(path).endswith(".gz"):
        return gzip.open(path, "wt", encoding="utf-8")
    return path.open("w", encoding="utf-8")


def write_csv_header(handle, fields: List[Dict[str, Any]]) -> None:
    headers = [field["name"] for field in fields]
    handle.write(",".join(headers) + "\n")


def csv_escape(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    if any(ch in text for ch in [",", "\"", "\n"]):
        text = "\"" + text.replace("\"", "\"\"") + "\""
    return text


def write_csv_row(handle, row_values: Iterable[Any]) -> None:
    serialised: List[str] = []
    for value in row_values:
        if isinstance(value, dict):
            serialised.append(csv_escape(json.dumps(value, ensure_ascii=False)))
        else:
            serialised.append(csv_escape(value))
    handle.write(",".join(serialised) + "\n")


def write_jsonl_row(handle, obj: Dict[str, Any]) -> None:
    handle.write(json.dumps(obj, ensure_ascii=False) + "\n")


def resolve_output_path(args: argparse.Namespace, fmt: str) -> Path:
    if args.out_file:
        return Path(args.out_file)

    base = args.output_name or "synthetic_events"
    suffix = f".{fmt}"
    if args.gzip and not base.endswith(".gz"):
        suffix += ".gz"
    path = Path(args.output_dir) / f"{base}{suffix}"
    return path


def maybe_clear_output(path: Path, fmt: str, enabled: bool) -> None:
    if not enabled:
        return
    directory = path.parent
    pattern = f"*.{fmt}" + (".gz" if path.suffix == ".gz" or path.name.endswith(".gz") else "")
    for candidate in directory.glob(pattern):
        try:
            candidate.unlink()
        except FileNotFoundError:
            continue


def main() -> None:
    args = parse_args()

    with open(args.config, "r", encoding="utf-8") as cfg_handle:
        config = json.load(cfg_handle)

    seed = args.seed if args.seed is not None else config.get("seed")
    if seed is not None:
        random.seed(int(seed))

    record_count = args.n if args.n is not None else int(config.get("entity_count", 1000))
    output_cfg = config.get("output", {})
    fmt = args.format if args.format is not None else output_cfg.get("format", "csv")
    if fmt not in {"csv", "jsonl"}:
        raise SystemExit(f"Unsupported output format: {fmt}")

    fields = config["schema"]["fields"]
    time_window = config.get(
        "time_window",
        {"start": "2025-01-01T00:00:00", "end": "2025-01-07T23:59:59"},
    )

    output_path = resolve_output_path(args, fmt)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    maybe_clear_output(output_path, fmt, args.clear_output)

    use_gzip = args.gzip or str(output_path).endswith(".gz")

    with open_out(output_path, use_gzip) as handle:
        if fmt == "csv":
            write_csv_header(handle, fields)
        for index in range(record_count):
            context: Dict[str, Any] = {}
            for field in fields:
                value = gen_field(field, context, index, record_count, time_window)
                context[field["name"]] = value
            if fmt == "csv":
                write_csv_row(handle, [context[field["name"]] for field in fields])
            else:
                write_jsonl_row(handle, context)

    descriptor = f"{fmt}{' (gzip)' if use_gzip else ''}"
    print(
        f"✅ Wrote {record_count} record(s) to {output_path} (format={descriptor})"
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # pragma: no cover - defensive logging for CLI usage
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
