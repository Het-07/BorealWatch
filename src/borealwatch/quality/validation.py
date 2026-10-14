"""Data-quality checks.

Checks in this module *report* problems; they never modify data. Bronze keeps
every source record, so fixing or dropping rows is a Silver-layer decision.

`validate_required_columns` and `validate_expected_columns` work on any
DataFrame with a `.columns` attribute (pandas or Spark). The remaining helpers
take Spark DataFrames.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def validate_required_columns(df, required_columns: Iterable[str]) -> bool:
    """Raise ValueError if any required column is missing."""
    missing = [column for column in required_columns if column not in df.columns]

    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    return True


def validate_expected_columns(df, expected_columns: Sequence[str]) -> bool:
    """Raise ValueError if the columns differ from the expected source contract.

    Catches both missing columns and unexpected new ones, which usually means
    the publisher changed the file layout.
    """
    actual = list(df.columns)
    missing = [c for c in expected_columns if c not in actual]
    unexpected = [c for c in actual if c not in expected_columns]

    if missing or unexpected:
        raise ValueError(
            f"Source columns do not match the expected contract. "
            f"Missing: {missing}. Unexpected: {unexpected}."
        )

    return True


def reconcile_row_counts(source_count: int, target_count: int, label: str = "rows") -> bool:
    """Raise ValueError if the target row count differs from the source."""
    if source_count != target_count:
        raise ValueError(
            f"Row-count reconciliation failed for {label}: "
            f"source={source_count:,}, target={target_count:,}"
        )

    return True


def count_nulls(df: DataFrame, columns: Iterable[str]) -> dict[str, int]:
    """Return the number of null values in each of the given columns."""
    columns = list(columns)
    if not columns:
        return {}

    row = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in columns]).first()

    return {c: int(row[c] or 0) for c in columns}


def count_duplicate_keys(df: DataFrame, key_columns: Sequence[str]) -> int:
    """Return how many rows share a key with at least one other row.

    Used to test business-key assumptions such as (YEAR, FIRE_NUMBER)
    before Silver relies on them.
    """
    duplicated = (
        df.groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .agg(F.sum("count").alias("rows"))
        .first()
    )

    return int(duplicated["rows"] or 0)
