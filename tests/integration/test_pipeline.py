import pytest

from borealwatch.ingestion.wildfire import (
    EXPECTED_COLUMNS,
    METADATA_COLUMNS,
    compute_sha256,
    format_summary,
    ingest_wildfire,
    main,
)


def test_bronze_ingestion_end_to_end(spark, tmp_path, wildfire_sample_csv) -> None:
    output = tmp_path / "bronze" / "wildfire"

    result = ingest_wildfire(spark, wildfire_sample_csv, output)

    assert result.rows_read == 10
    assert result.rows_written == 10
    assert result.source_columns == 50
    assert result.source_sha256 == compute_sha256(wildfire_sample_csv)
    assert result.key_nulls == {"YEAR": 0, "FIRE_NUMBER": 0, "LATITUDE": 0, "LONGITUDE": 0}
    # PWF001 repeats across years but (YEAR, FIRE_NUMBER) is unique in the sample.
    assert result.duplicate_business_key_rows == 0

    bronze = spark.read.parquet(str(output))
    assert bronze.columns == list(EXPECTED_COLUMNS) + list(METADATA_COLUMNS)
    assert bronze.select("_ingestion_run_id").distinct().count() == 1
    assert "Rows written:    10 (reconciled)" in format_summary(result)


def test_bronze_rerun_is_idempotent(spark, tmp_path, wildfire_sample_csv) -> None:
    output = tmp_path / "bronze" / "wildfire"

    first = ingest_wildfire(spark, wildfire_sample_csv, output)
    second = ingest_wildfire(spark, wildfire_sample_csv, output)

    assert first.run_id != second.run_id
    bronze = spark.read.parquet(str(output))
    assert bronze.count() == 10
    assert [
        r._ingestion_run_id for r in bronze.select("_ingestion_run_id").distinct().collect()
    ] == [second.run_id]


def test_missing_source_file_raises(spark, tmp_path) -> None:
    with pytest.raises(FileNotFoundError, match="Raw wildfire file not found"):
        ingest_wildfire(spark, tmp_path / "missing.csv", tmp_path / "out")


def test_main_returns_error_code_for_missing_file(tmp_path) -> None:
    exit_code = main(["--source", str(tmp_path / "missing.csv"), "--output", str(tmp_path / "out")])
    assert exit_code == 1
