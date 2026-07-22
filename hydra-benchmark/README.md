# Hydra Benchmark Example

This example demonstrates how to use Hydra for configuration management in a Metaflow flow that benchmarks different data processing backends.

## Overview

The flow benchmarks three different data processing libraries:
- Pandas
- Polars
- DuckDB

Each backend processes the same dataset (yellow taxi trip data) to calculate hourly total amounts, allowing for performance comparison.

## Structure

```
hydra-benchmark/
├── benchmark_flow.py        # Metaflow flow definition
├── benchmark_runner.py      # Hydra entrypoint
├── config.yaml              # Default configuration
├── backend/
│   ├── pandas/
│   │   ├── benchmark.py     # Pandas implementation
│   │   └── pandas.yaml      # Pandas dependencies
│   ├── polars/
│   │   ├── benchmark.py     # Polars implementation
│   │   └── polars.yaml      # Polars dependencies
│   └── duckdb/
│       ├── benchmark.py     # DuckDB implementation
│       └── duckdb.yaml      # DuckDB dependencies
└└── __init__.py             # Backend package initializer
```

## How It Works

1. **Configuration**: Hydra loads configuration from `config.yaml` and command-line overrides
2. **Serialization**: The configuration is serialized to JSON and passed to Metaflow via `METAFLOW_FLOW_CONFIG_VALUE` environment variable
3. **Execution**: Metaflow runs `benchmark_flow.py` which:
   - Loads the serialized configuration
   - Dynamically imports the selected backend's benchmark module
   - Downloads and processes the parquet files
   - Executes the benchmark function
   - Reports timing and results

## Running Locally

To run the benchmark with a specific backend:

```bash
python benchmark_runner.py backend.name=pandas
python benchmark_runner.py backend.name=polars
python benchmark_runner.py backend.name=duckdb
```

To force local execution (instead of Kubernetes):

```bash
python benchmark_runner.py remote=null
```

## Viewing Results

Results are printed to stdout showing execution time for each backend:

```
Backend pandas took 1234ms
```

Detailed Metaflow logs and artifacts can be inspected using standard Metaflow commands.

## Requirements

- Python 3.8+
- Metaflow
- Hydra
- Respective backend dependencies (specified in backend/*.yaml files)

Note: This example is designed to run on Kubernetes by default (via Metaflow's `@kubernetes` decorator), but can be forced to run locally for development/testing.