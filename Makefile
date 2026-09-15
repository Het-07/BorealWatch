.PHONY: install test format lint run-wildfire

install:
	python -m pip install -r requirements.txt

install-dev:
	python -m pip install -r requirements.txt

test:
	pytest

format:
	python -m ruff format src tests

lint:
	python -m ruff check src tests

run-wildfire:
	python -m borealwatch.ingestion.wildfire
