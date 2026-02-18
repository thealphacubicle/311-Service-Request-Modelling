# Data

### Data Loading

```bash
uv run scripts/utility/scrape.py
```

This will pull 311 service request data for 2019–2024 from the Analyze Boston API and save it to `data/raw/311_2019_2024.parquet`. Expect roughly 1.1M records and a few minutes of runtime depending on your connection.
