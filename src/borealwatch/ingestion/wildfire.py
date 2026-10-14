"""Bronze ingestion for the Alberta historical wildfire dataset.

Raw CSV (unchanged)  ->  Bronze Parquet (all source columns + provenance)

Bronze design decisions:
- Every source column is read as a string. Bronze is lossless; typing,
  timestamp parsing and unit checks happen explicitly in Silver.
- The column layout is checked against an explicit 50-column contract
  instead of relying on schema inference.
- Provenance columns record when, from which file, and from which exact file
  contents (SHA-256) each row was ingested.
- Output is overwritten on each run, so reruns are idempotent.
- Quality checks report; they do not drop or alter rows.

Run from the project root:
    python -m borealwatch.ingestion.wildfire
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from borealwatch.quality.validation import (
    count_duplicate_keys,
    count_nulls,
    reconcile_row_counts,
    validate_expected_columns,
)
from borealwatch.utils.paths import BRONZE_DIR, PROJECT_ROOT
from borealwatch.utils.spark import get_spark_session

logger = logging.getLogger(__name__)

SOURCE_NAME = "alberta_historical_wildfires"
RAW_FILENAME = "alberta_wildfires_2006_2025.csv"

# Source contract: the 50 columns of the Government of Alberta file, in order.
EXPECTED_COLUMNS: tuple[str, ...] = (
    "YEAR",
    "FIRE_NUMBER",
    "FIRE_NAME",
    "CURRENT_SIZE",
    "SIZE_CLASS",
    "LATITUDE",
    "LONGITUDE",
    "FIRE_ORIGIN",
    "GENERAL_CAUSE",
    "INDUSTRY_IDENTIFIER",
    "RESPONSIBLE_GROUP",
    "ACTIVITY_CLASS",
    "TRUE_CAUSE",
    "FIRE_START_DATE",
    "DETECTION_AGENT_TYPE",
    "DETECTION_AGENT",
    "DISCOVERED_DATE",
    "DISCOVERED_SIZE",
    "REPORTED_DATE",
    "DISPATCHED_RESOURCE",
    "DISPATCH_DATE",
    "START_FOR_FIRE_DATE",
    "ASSESSMENT_RESOURCE",
    "ASSESSMENT_DATETIME",
    "ASSESSMENT_HECTARES",
    "FIRE_SPREAD_RATE",
    "FIRE_TYPE",
    "FIRE_POSITION_ON_SLOPE",
    "WEATHER_CONDITIONS_OVER_FIRE",
    "TEMPERATURE",
    "RELATIVE_HUMIDITY",
    "WIND_DIRECTION",
    "WIND_SPEED",
    "FUEL_TYPE",
    "INITIAL_ACTION_BY",
    "IA_ARRIVAL_AT_FIRE_DATE",
    "IA_ACCESS",
    "FIRE_FIGHTING_START_DATE",
    "FIRE_FIGHTING_START_SIZE",
    "BUCKETING_ON_FIRE",
    "DISTANCE_FROM_WATER_SOURCE",
    "FIRST_BUCKET_DROP_DATE",
    "FIRST_BH_DATE",
    "FIRST_BH_SIZE",
    "FIRST_UC_DATE",
    "FIRST_UC_SIZE",
    "FIRST_TO_DATE",
    "FIRST_TO_SIZE",
    "FIRST_EX_DATE",
    "FIRST_EX_SIZE_PERIMETER",
)

# Columns that every record needs to be usable downstream. Nulls are reported.
KEY_COLUMNS: tuple[str, ...] = ("YEAR", "FIRE_NUMBER", "LATITUDE", "LONGITUDE")

# Candidate business key, to be confirmed before Silver deduplicates on it.
CANDIDATE_BUSINESS_KEY: tuple[str, ...] = ("YEAR", "FIRE_NUMBER")

METADATA_COLUMNS: tuple[str, ...] = (
    "_ingested_at",
    "_ingestion_run_id",
    "_source_name",
    "_source_file",
    "_source_sha256",
)


@dataclass
class IngestionResult:
    """Summary of one Bronze ingestion run."""

    run_id: str
    source_file: Path
    source_sha256: str
    output_path: Path
    rows_read: int
    rows_written: int
    source_columns: int
    key_nulls: dict[str, int] = field(default_factory=dict)
    duplicate_business_key_rows: int = 0


def get_raw_wildfire_path(project_root: Path = PROJECT_ROOT) -> Path:
    """Return the raw wildfire data directory."""
    return project_root / "data" / "raw" / "wildfire"


def get_raw_wildfire_file(project_root: Path = PROJECT_ROOT) -> Path:
    """Return the expected raw wildfire CSV path."""
    return get_raw_wildfire_path(project_root) / RAW_FILENAME


def ensure_output_directory(output_path: Path | None = None) -> Path:
    """Create the Bronze wildfire directory if needed and return it."""
    path = Path(output_path) if output_path is not None else BRONZE_DIR / "wildfire"
    path.mkdir(parents=True, exist_ok=True)
    return path


def compute_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """Return the SHA-256 hex digest of a file."""
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_raw_wildfire(spark: SparkSession, source_file: Path) -> DataFrame:
    """Read the raw CSV with every column as a string and check the contract."""
    df = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "FAILFAST")
        .csv(str(source_file))
    )

    validate_expected_columns(df, EXPECTED_COLUMNS)
    return df


def add_ingestion_metadata(
    df: DataFrame,
    *,
    run_id: str,
    ingested_at: datetime,
    source_file: Path,
    source_sha256: str,
) -> DataFrame:
    """Append provenance columns to every row."""
    return (
        df.withColumn("_ingested_at", F.lit(ingested_at).cast("timestamp"))
        .withColumn("_ingestion_run_id", F.lit(run_id))
        .withColumn("_source_name", F.lit(SOURCE_NAME))
        .withColumn("_source_file", F.lit(Path(source_file).name))
        .withColumn("_source_sha256", F.lit(source_sha256))
    )


def ingest_wildfire(
    spark: SparkSession,
    source_file: Path,
    output_path: Path,
) -> IngestionResult:
    """Ingest the raw wildfire CSV into Bronze Parquet and verify the result."""
    source_file = Path(source_file)
    if not source_file.is_file():
        raise FileNotFoundError(
            f"Raw wildfire file not found: {source_file}. "
            f"Download it into {get_raw_wildfire_path()} first."
        )

    run_id = str(uuid.uuid4())
    ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)
    source_sha256 = compute_sha256(source_file)
    output_path = ensure_output_directory(output_path)

    logger.info("Reading %s", source_file)
    raw_df = read_raw_wildfire(spark, source_file)
    rows_read = raw_df.count()

    bronze_df = add_ingestion_metadata(
        raw_df,
        run_id=run_id,
        ingested_at=ingested_at,
        source_file=source_file,
        source_sha256=source_sha256,
    )

    logger.info("Writing Bronze Parquet to %s", output_path)
    bronze_df.write.mode("overwrite").parquet(str(output_path))

    # Verify by reading the written output back, not by trusting the write.
    written_df = spark.read.parquet(str(output_path))
    rows_written = written_df.count()
    reconcile_row_counts(rows_read, rows_written, label="raw CSV -> Bronze")
    validate_expected_columns(written_df, EXPECTED_COLUMNS + METADATA_COLUMNS)

    return IngestionResult(
        run_id=run_id,
        source_file=source_file,
        source_sha256=source_sha256,
        output_path=output_path,
        rows_read=rows_read,
        rows_written=rows_written,
        source_columns=len(raw_df.columns),
        key_nulls=count_nulls(written_df, KEY_COLUMNS),
        duplicate_business_key_rows=count_duplicate_keys(written_df, CANDIDATE_BUSINESS_KEY),
    )


def format_summary(result: IngestionResult) -> str:
    """Return a human-readable summary of an ingestion run."""
    nulls = ", ".join(f"{k}={v:,}" for k, v in result.key_nulls.items())
    key = " + ".join(CANDIDATE_BUSINESS_KEY)
    return "\n".join(
        [
            "Bronze ingestion complete",
            f"  Run ID:          {result.run_id}",
            f"  Source file:     {result.source_file}",
            f"  Source SHA-256:  {result.source_sha256}",
            f"  Output:          {result.output_path}",
            f"  Rows read:       {result.rows_read:,}",
            f"  Rows written:    {result.rows_written:,} (reconciled)",
            f"  Source columns:  {result.source_columns}",
            f"  Key-field nulls: {nulls}",
            f"  Rows sharing a ({key}) key: {result.duplicate_business_key_rows:,}",
        ]
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest Alberta wildfire CSV into Bronze.")
    parser.add_argument(
        "--source",
        type=Path,
        default=get_raw_wildfire_file(),
        help="Raw wildfire CSV (default: data/raw/wildfire/%(default)s).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=BRONZE_DIR / "wildfire",
        help="Bronze output directory (default: %(default)s).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    args = parse_args(argv)

    # Fail fast, before paying Spark's start-up cost.
    if not args.source.is_file():
        logger.error(
            "Raw wildfire file not found: %s. Download it into %s first.",
            args.source,
            get_raw_wildfire_path(),
        )
        return 1

    spark = get_spark_session("borealwatch-wildfire-bronze")
    spark.sparkContext.setLogLevel("WARN")
    try:
        result = ingest_wildfire(spark, args.source, args.output)
    except (FileNotFoundError, ValueError) as error:
        logger.error("Bronze ingestion failed: %s", error)
        return 1
    finally:
        spark.stop()

    print(format_summary(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())
