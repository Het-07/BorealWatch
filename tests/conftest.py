from pathlib import Path

import pytest

from borealwatch.utils.spark import get_spark_session

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def spark():
    session = get_spark_session("borealwatch-tests", master="local[1]")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def wildfire_sample_csv() -> Path:
    return FIXTURES_DIR / "alberta_wildfires_sample.csv"
