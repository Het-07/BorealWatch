import pandas as pd
import pytest

from borealwatch.quality.validation import (
    count_duplicate_keys,
    count_nulls,
    reconcile_row_counts,
    validate_expected_columns,
    validate_required_columns,
)


def test_required_columns_present():
    df = pd.DataFrame(
        {
            "latitude": [45.0],
            "longitude": [-63.0],
        }
    )

    assert validate_required_columns(
        df,
        ["latitude", "longitude"],
    )


def test_required_columns_missing():
    df = pd.DataFrame(
        {
            "latitude": [45.0],
        }
    )

    with pytest.raises(ValueError):
        validate_required_columns(
            df,
            ["latitude", "longitude"],
        )


def test_expected_columns_match():
    df = pd.DataFrame(columns=["a", "b"])
    assert validate_expected_columns(df, ["a", "b"])


def test_expected_columns_reports_missing_and_unexpected():
    df = pd.DataFrame(columns=["a", "c"])

    with pytest.raises(ValueError, match=r"Missing: \['b'\]. Unexpected: \['c'\]"):
        validate_expected_columns(df, ["a", "b"])


def test_reconcile_row_counts():
    assert reconcile_row_counts(10, 10)

    with pytest.raises(ValueError, match="source=10, target=9"):
        reconcile_row_counts(10, 9)


def test_count_nulls(spark):
    df = spark.createDataFrame(
        [("2006", None), ("2007", "PWF001"), (None, None)],
        "YEAR string, FIRE_NUMBER string",
    )

    assert count_nulls(df, ["YEAR", "FIRE_NUMBER"]) == {"YEAR": 1, "FIRE_NUMBER": 2}
    assert count_nulls(df, []) == {}


def test_count_duplicate_keys(spark):
    df = spark.createDataFrame(
        [
            ("2006", "PWF001"),
            ("2007", "PWF001"),
            ("2007", "PWF001"),
            ("2008", "EWF002"),
        ],
        "YEAR string, FIRE_NUMBER string",
    )

    # FIRE_NUMBER alone repeats; (YEAR, FIRE_NUMBER) repeats only in 2007.
    assert count_duplicate_keys(df, ["FIRE_NUMBER"]) == 3
    assert count_duplicate_keys(df, ["YEAR", "FIRE_NUMBER"]) == 2
    assert count_duplicate_keys(df, ["YEAR", "FIRE_NUMBER"]) > 0
