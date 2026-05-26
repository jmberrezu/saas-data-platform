.PHONY: install test lint run clean

install:
	python -m pip install --upgrade pip
	pip install -r requirements.txt

test:
	python -m pytest tests/ -v

lint:
	flake8 src/ tests/ --ignore=E501

run:
	python -m src.saas_pipeline.orchestrator --tenant ec

run-all:
	python -m src.saas_pipeline.orchestrator --tenant all

clean:
	rm -rf data/bronze/*
	rm -rf data/silver/*
	rm -rf data/gold/*
	rm -rf data/silver_quarantine/*
	rm -rf data/shared/*