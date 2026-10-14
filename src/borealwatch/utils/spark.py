"""Shared Spark session helpers."""

from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "borealwatch", master: str = "local[*]") -> SparkSession:
    """Create (or reuse) a local Spark session with project defaults.

    - UTC session time zone so ingestion timestamps are unambiguous.
    - Few shuffle partitions: the datasets are small and run on a laptop.
    - Driver bound to localhost, which avoids common hostname errors on macOS.
    """
    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
