from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, expr, when
from pyspark.sql.functions import sum as spark_sum

from core.config import settings

REQUIRED_FIELDS = [
    "unique_key",
    "created_date",
    "due_date",
    "closed_date",
    "agency",
    "complaint_type",
    "borough",
]

DATE_FIELDS = ["created_date", "due_date", "closed_date"]

FIELD_ALIASES = {
    "unique_key": ["unique_key", "Unique Key", "uniqueKey", "complaint_id"],
    "created_date": ["created_date", "created_dt", "created_ts", "createdDate"],
    "due_date": ["due_date", "due_dt", "due_ts", "dueDate"],
    "closed_date": ["closed_date", "closed_dt", "closed_ts", "closedDate"],
    "agency": ["agency", "agency_name"],
    "complaint_type": ["complaint_type", "complaint", "complaint_category"],
    "borough": ["borough", "borough_name"],
}

HIGH_NULL_WARNING_THRESHOLD = 0.50


@dataclass(frozen=True)
class ValidationQualityMetrics:
    required_fields: list[str]
    available_fields: list[str]
    missing_fields: list[str]
    alias_resolution: dict[str, str]
    null_counts: dict[str, int] = field(default_factory=dict)
    null_rates: dict[str, float] = field(default_factory=dict)
    malformed_date_counts: dict[str, int] = field(default_factory=dict)
    high_null_fields: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ValidationResult:
    is_valid: bool
    errors: list[str]
    warnings: list[str]
    row_count: int
    quality_metrics: ValidationQualityMetrics

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def normalize_schema(df: DataFrame) -> tuple[DataFrame, list[str]]:
    """Rename accepted aliases to canonical field names.

    The first matching alias for each canonical field wins. Existing canonical
    columns are preserved.
    """
    normalized_df = df
    warnings: list[str] = []

    for canonical_name, aliases in FIELD_ALIASES.items():
        if canonical_name in normalized_df.columns:
            continue

        for alias in aliases:
            if alias == canonical_name:
                continue

            if alias in normalized_df.columns:
                normalized_df = normalized_df.withColumnRenamed(alias, canonical_name)
                warnings.append(f"Accepted alias '{alias}' was normalized to '{canonical_name}'.")
                break

    return normalized_df, warnings


def _build_null_aggregations(df: DataFrame, fields: list[str]):
    return [
        spark_sum(when(col(field_name).isNull(), 1).otherwise(0)).alias(field_name)
        for field_name in fields
    ]


def _build_malformed_date_aggregations(df: DataFrame, fields: list[str]):
    def _try_parse_expr(field_name: str) -> str:
        escaped_formats = [fmt.replace("'", "''") for fmt in settings.TIMESTAMP_FORMATS]
        if not escaped_formats:
            return f"try_to_timestamp(`{field_name}`)"
        return (
            "coalesce("
            + ", ".join([f"try_to_timestamp(`{field_name}`, '{fmt}')" for fmt in escaped_formats])
            + ")"
        )

    return [
        spark_sum(
            when(
                col(field_name).isNotNull() & expr(_try_parse_expr(field_name)).isNull(),
                1,
            ).otherwise(0)
        ).alias(f"__malformed_{field_name}")
        for field_name in fields
    ]


def validate_schema(df: DataFrame, warnings: list[str] | None = None) -> ValidationResult:
    warnings = list(warnings or [])
    errors: list[str] = []

    available_fields = list(df.columns)
    missing_fields = [
        field_name for field_name in REQUIRED_FIELDS if field_name not in available_fields
    ]

    present_required_fields = [
        field_name for field_name in REQUIRED_FIELDS if field_name in available_fields
    ]
    present_date_fields = [
        field_name for field_name in DATE_FIELDS if field_name in available_fields
    ]

    agg_row = df.agg(
        count("*").alias("__row_count"),
        *_build_null_aggregations(df, present_required_fields),
        *_build_malformed_date_aggregations(df, present_date_fields),
    ).collect()[0]
    row_count = int(agg_row["__row_count"] or 0)

    if row_count == 0:
        errors.append("Input dataset is empty.")

    if missing_fields:
        errors.append("Missing required fields: " + ", ".join(missing_fields))

    null_counts: dict[str, int] = {field_name: 0 for field_name in REQUIRED_FIELDS}
    null_rates: dict[str, float] = {field_name: 0.0 for field_name in REQUIRED_FIELDS}
    malformed_date_counts: dict[str, int] = {field_name: 0 for field_name in DATE_FIELDS}
    high_null_fields: list[str] = []

    if row_count > 0 and not missing_fields:
        for field_name in present_required_fields:
            null_counts[field_name] = int(agg_row[field_name] or 0)
            null_rates[field_name] = null_counts[field_name] / row_count if row_count else 0.0

            if null_counts[field_name] == row_count:
                errors.append(f"All records have null {field_name}.")
            elif null_rates[field_name] >= HIGH_NULL_WARNING_THRESHOLD:
                high_null_fields.append(field_name)
                warnings.append(
                    f"High null rate detected in '{field_name}' ({null_rates[field_name]:.1%})."
                )

        for field_name in present_date_fields:
            malformed_date_counts[field_name] = int(agg_row[f"__malformed_{field_name}"] or 0)
            if malformed_date_counts[field_name] > 0:
                errors.append(
                    f"Field '{field_name}' contains {malformed_date_counts[field_name]} malformed timestamp value(s)."
                )

    # Rebuild alias resolution from warnings so the result remains typed and serializable.
    alias_resolution: dict[str, str] = {}
    for warning in warnings:
        if warning.startswith("Accepted alias '"):
            parts = warning.split("'")
            if len(parts) >= 4:
                alias_resolution[parts[1]] = parts[3]

    metrics = ValidationQualityMetrics(
        required_fields=list(REQUIRED_FIELDS),
        available_fields=available_fields,
        missing_fields=missing_fields,
        alias_resolution=alias_resolution,
        null_counts=null_counts,
        null_rates=null_rates,
        malformed_date_counts=malformed_date_counts,
        high_null_fields=high_null_fields,
    )

    return ValidationResult(
        is_valid=not errors,
        errors=errors,
        warnings=warnings,
        row_count=row_count,
        quality_metrics=metrics,
    )
