"""BorealWatch batch pipeline entry point.

Runs each enabled stage in order. Only Bronze wildfire ingestion exists so far;
Silver and Gold stages will be added here as they are built.

Run from the project root:
    python -m borealwatch.pipelines.batch_pipeline
"""

from __future__ import annotations

import logging
import sys

from borealwatch.ingestion.wildfire import format_summary, get_raw_wildfire_file, ingest_wildfire
from borealwatch.utils.paths import BRONZE_DIR, GOLD_DIR, RAW_DIR, SILVER_DIR
from borealwatch.utils.spark import get_spark_session

logger = logging.getLogger(__name__)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    for path in (RAW_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR):
        path.mkdir(parents=True, exist_ok=True)

    spark = get_spark_session("borealwatch-batch")
    spark.sparkContext.setLogLevel("WARN")
    try:
        result = ingest_wildfire(spark, get_raw_wildfire_file(), BRONZE_DIR / "wildfire")
    except (FileNotFoundError, ValueError) as error:
        logger.error("Pipeline failed at Bronze wildfire ingestion: %s", error)
        return 1
    finally:
        spark.stop()

    print(format_summary(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())
