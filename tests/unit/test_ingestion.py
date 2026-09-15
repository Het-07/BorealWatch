from pathlib import Path

from borealwatch.ingestion.wildfire import ensure_output_directory


def test_wildfire_output_directory_exists() -> None:
    path = ensure_output_directory()
    assert Path(path).exists()
    assert Path(path).is_dir()
