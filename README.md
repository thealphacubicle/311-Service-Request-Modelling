# Boston 311 Service Demand

## Introduction

This repository forecasts neighborhood-level demand for Boston’s 311 service requests using public data from [Analyze Boston](https://data.boston.gov/dataset/311-service-requests) (from 2019–2024 with around 1.1M records). 

The workflow is:

- **Python** — Ingestion, cleaning, and feature work (Polars/Pandas), exploratory analysis in Jupyter notebooks, and scripted pulls from the city API.
- **R** — Bayesian count models in `notebooks/05_bayesian_model.rmd` using **brms** (negative binomial and related families), with outputs such as metrics and figures written under `results/` and serialized models under `models/` when you run the notebook.
- **Web app** — A small **Next.js** app under `app/frontend` that reads CSV metrics from `results/metrics` and provides an interactive comparison of the Bayesian model runs (families, training versions, MAE/RMSE, coverage, WAIC/LOO-IC, etc.).

## Quickstart

Prerequisites: **Python 3.10+**, **Node.js** (for npm), and **R** (for the Bayesian notebook). The Python toolchain is managed with [**uv**](https://docs.astral.sh/uv/).

### Python dependencies

Install `uv` if you do not already have it (pick one):

```bash
pip install uv
# or: brew install uv
# or: curl -LsSf https://astral.sh/uv/install.sh | sh
```

From the repository root:

```bash
uv venv
uv sync
```

`uv sync` installs everything in `pyproject.toml` / `uv.lock` into `.venv`. Use `uv run …` for scripts (for example, `uv run scripts/utility/scrape.py` to fetch raw data — see `data/README.md`).

### npm (frontend)

The results UI lives in `app/frontend`. Install JavaScript dependencies from that directory:

```bash
cd app/frontend
npm install
```

Useful scripts (run in `app/frontend`): `npm run dev` (local dev server), `npm run build`, `npm run start`.

### R

Bayesian modeling is in `notebooks/05_bayesian_model.rmd`. Install the CRAN packages used there (one-time) or just uncomment the installation cell and run it there:

```r
install.packages(c(
  "brms", "readr", "rprojroot", "dplyr", "nanoparquet",
  "ggplot2", "parallel"
))
```

### NOTE:
To populate the repo with the required data and other files, ensure you run each of the files in this order: 

#### 1. Populating Raw Data
```bash
# Runs the raw data scraping script and saves in /data/raw
uv run python scripts/utility/scrape.py
```

#### 2. Run each of the notebooks (ordered by numeric prefix) in `notebooks/`

### Deployment
The frontend for this project is deployed on [Vercel:](https://311-service-request-modelling.vercel.app/)
