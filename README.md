# Boston 311 Service Demand

Forecasting municipal service request demand across Boston neighborhoods using time series and Bayesian modeling. Built on data from [Analyze Boston](https://data.boston.gov/dataset/311-service-requests) covering 2019–2024 (~1.1M records).

## Quick Start

### 1. Install uv

Choose whichever method suits your setup:

```bash
# pip
pip install uv

# Homebrew (macOS)
brew install uv

# curl (macOS/Linux)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows (PowerShell)
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### 2. Create a uv environment

From the project root:

```bash
uv venv
```

This creates a `.venv` folder in the project directory.

### 3. Sync dependencies

```bash
uv sync
```

This installs all dependencies from `pyproject.toml` and `uv.lock` exactly as specified.

### 4. Load the data

See data/READ.me to locally scrape and load the data!
