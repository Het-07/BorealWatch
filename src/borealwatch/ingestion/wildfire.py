from pathlib import Path


def get_raw_wildfire_path(project_root: Path) -> Path:
    # Return the raw wildfire data directory.
    return project_root / "data" / "raw" / "wildfire"


def main() -> None:
    # Initialize wildfire ingestion.
    project_root = Path(__file__).resolve().parents[3]
    raw_path = get_raw_wildfire_path(project_root)

    raw_path.mkdir(parents=True, exist_ok=True)

    print(f"Wildfire raw data directory: {raw_path}")


if __name__ == "__main__":
    main()