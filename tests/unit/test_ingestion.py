from datetime import datetime
from pathlib import Path

import pytest

from borealwatch.ingestion.wildfire import (
    EXPECTED_COLUMNS,
    METADATA_COLUMNS,
    add_ingestion_metadata,
    compute_sha256,
    ensure_output_directory,
    read_raw_wildfire,
)


def test_wildfire_output_directory_exists() -> None:
    path = ensure_output_directory()
    assert Path(path).exists()
    assert Path(path).is_dir()


def test_output_directory_accepts_custom_path(tmp_path) -> None:
    target = tmp_path / "bronze" / "wildfire"
    assert ensure_output_directory(target) == target
    assert target.is_dir()


def test_read_raw_keeps_all_columns_as_strings(spark, wildfire_sample_csv) -> None:
    df = read_raw_wildfire(spark, wildfire_sample_csv)

    assert df.columns == list(EXPECTED_COLUMNS)
    assert len(df.columns) == 50
    assert {dtype for _, dtype in df.dtypes} == {"string"}
    assert df.count() == 10


def test_read_raw_preserves_source_values(spark, wildfire_sample_csv) -> None:
    df = read_raw_wildfire(spark, wildfire_sample_csv)
    rows = {(r.YEAR, r.FIRE_NUMBER): r for r in df.collect()}

    # Quoted value with a comma stays intact.
    assert rows[("2007", "PWF001")].FIRE_NAME == "Smith, Lake Complex"
    # Suspicious values are kept as-is for Silver to judge.
    assert rows[("2006", "EWF002")].FIRE_SPREAD_RATE == "-1"
    assert rows[("2016", "MWF009")].CURRENT_SIZE == "577646.8"
    # Timestamps are untouched strings.
    assert rows[("2006", "PWF001")].FIRE_START_DATE == "2006-04-02 22:00"
    # Empty CSV fields become nulls.
    assert rows[("2006", "PWF001")].FIRE_NAME is None


def test_read_raw_rejects_changed_layout(spark, tmp_path, wildfire_sample_csv) -> None:
    lines = wildfire_sample_csv.read_text().splitlines()
    lines[0] = lines[0].replace("FIRE_NUMBER", "FIRE_ID")
    bad_file = tmp_path / "bad.csv"
    bad_file.write_text("\n".join(lines) + "\n")

    with pytest.raises(ValueError, match="FIRE_NUMBER"):
        read_raw_wildfire(spark, bad_file)


def test_add_ingestion_metadata(spark, wildfire_sample_csv) -> None:
    df = read_raw_wildfire(spark, wildfire_sample_csv)
    ingested_at = datetime(2026, 10, 14, 4, 0, 0)

    result = add_ingestion_metadata(
        df,
        run_id="test-run",
        ingested_at=ingested_at,
        source_file=wildfire_sample_csv,
        source_sha256="abc123",
    )

    assert result.columns == list(EXPECTED_COLUMNS) + list(METADATA_COLUMNS)
    row = result.first()
    assert row._ingested_at == ingested_at
    assert row._ingestion_run_id == "test-run"
    assert row._source_file == "alberta_wildfires_sample.csv"
    assert row._source_sha256 == "abc123"


def test_compute_sha256_is_stable(tmp_path) -> None:
    file = tmp_path / "data.csv"
    file.write_text("a,b\n1,2\n")

    first = compute_sha256(file)
    assert first == compute_sha256(file)
    assert len(first) == 64

    file.write_text("a,b\n1,3\n")
    assert compute_sha256(file) != first
