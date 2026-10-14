from pathlib import Path


def main() -> None:
    """Run the BorealWatch batch pipeline."""
    project_root = Path(__file__).resolve().parents[3]

    raw_path = project_root / "data" / "raw"
    bronze_path = project_root / "data" / "bronze"
    silver_path = project_root / "data" / "silver"
    gold_path = project_root / "data" / "gold"

    for path in [raw_path, bronze_path, silver_path, gold_path]:
        path.mkdir(parents=True, exist_ok=True)

    print("BorealWatch batch pipeline initialized.")
    print(f"Raw layer: {raw_path}")
    print(f"Bronze layer: {bronze_path}")
    print(f"Silver layer: {silver_path}")
    print(f"Gold layer: {gold_path}")


if __name__ == "__main__":
    main()