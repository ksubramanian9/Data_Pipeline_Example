"""Utilities to read dataset schema metadata shared by batch components."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class SchemaMetadata:
    """Derived view of the dataset schema for downstream processing."""

    dataset_name: str
    timestamp_field: str
    dimensions: List[str]
    numeric_fields: List[str]
    boolean_fields: List[str]
    currency: Optional[str]

    @property
    def date_column(self) -> str:
        return f"{self.timestamp_field}_date"

    @property
    def primary_metric(self) -> Optional[str]:
        return self.numeric_fields[0] if self.numeric_fields else None

    @property
    def value_column(self) -> str:
        metric = self.primary_metric
        if metric:
            return f"sum_{metric}"
        return "record_count"


def _normalise_name(name: str) -> str:
    return name.strip().lower()


def load_schema_metadata(path: str | Path) -> SchemaMetadata:
    path = Path(path)
    with path.open("r", encoding="utf-8") as handle:
        raw: Dict[str, Any] = json.load(handle)

    fields = raw.get("schema", {}).get("fields", [])
    if not fields:
        raise ValueError(f"No fields declared in schema config: {path}")

    analysis_cfg = raw.get("analysis", {})

    def list_from_cfg(key: str) -> Optional[List[str]]:
        value = analysis_cfg.get(key)
        if not value:
            return None
        return [_normalise_name(str(item)) for item in value]

    timestamp_field = analysis_cfg.get("timestamp_field")
    if timestamp_field:
        timestamp_field = _normalise_name(str(timestamp_field))
    else:
        for field in fields:
            if field.get("type") == "datetime":
                timestamp_field = _normalise_name(field["name"])
                break
    if not timestamp_field:
        raise ValueError("Schema must declare at least one datetime field")

    dimensions = list_from_cfg("dimensions")
    if dimensions is None:
        dimensions = [
            _normalise_name(field["name"])
            for field in fields
            if field.get("type") in {"category", "string"}
        ]

    numeric_fields = list_from_cfg("numeric_fields")
    if numeric_fields is None:
        numeric_fields = [
            _normalise_name(field["name"])
            for field in fields
            if field.get("type") in {"float", "int"}
        ]

    boolean_fields = list_from_cfg("boolean_fields")
    if boolean_fields is None:
        boolean_fields = [
            _normalise_name(field["name"])
            for field in fields
            if field.get("type") == "bool"
        ]

    dataset_name = analysis_cfg.get("name") or raw.get("name") or "Synthetic Dataset"
    currency = analysis_cfg.get("currency")

    return SchemaMetadata(
        dataset_name=str(dataset_name),
        timestamp_field=timestamp_field,
        dimensions=dimensions,
        numeric_fields=numeric_fields,
        boolean_fields=boolean_fields,
        currency=str(currency) if currency else None,
    )
