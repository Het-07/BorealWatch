import pandas as pd
import pytest

from borealwatch.quality.validation import validate_required_columns


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