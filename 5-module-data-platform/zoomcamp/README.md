# Overview - End-to-End Data Platform

## Pipeline Skeleton

The required parts of a Bruin project are:
- `.bruin.yml` in the root directory
- `pipeline.yml` in the `pipeline/` directory (or in the root directory if you keep everything flat)
- `assets/` folder next to `pipeline.yml` containing your Python, SQL, and YAML asset files

```text
zoomcamp/
├── .bruin.yml                              # Environments + connections (local DuckDB, BigQuery, etc.)
├── README.md                               # Learning goals, workflow, best practices
└── pipeline/
    ├── pipeline.yml                        # Pipeline name, schedule, variables
    └── assets/
        ├── ingestion/
        │   ├── trips.py                    # Python ingestion
        │   ├── requirements.txt            # Python dependencies for ingestion
        │   ├── payment_lookup.asset.yml    # Seed asset definition
        │   └── payment_lookup.csv          # Seed data
        ├── staging/
        │   └── trips.sql                   # Clean and transform
        └── reports/
            └── trips_report.sql            # Aggregation for analytics
```

### AI-Assisted Workflow

**Recommended: Hybrid Approach**

For the best learning experience, consider a hybrid approach where you do the initial setup yourself, then let AI help with more complex parts:

1. **You do**: Install CLI, run `bruin init`, explore the generated files
2. **AI helps**: Configure connections, explain materialization strategies
3. **You do**: Create your first simple asset (e.g., the seed CSV)
4. **AI helps**: Build the Python ingestion and complex SQL transformations
5. **You do**: Run and validate, inspect the data
6. **AI helps**: Debug issues, add quality checks, optimize

This approach ensures you understand the fundamentals while leveraging AI for productivity.

**Layer-by-Layer Prompts**

Instead of building everything at once, progress through each layer:

**Layer 1 - Configuration:**
```text
Help me configure my Bruin project:
1. Set up `.bruin.yml` with a DuckDB connection named `duckdb-default`
2. Configure `pipeline.yml` with name, schedule, and a `taxi_types` variable
Reference: @pipeline/pipeline.yml
```

**Layer 2 - Ingestion:**
```text
Build the ingestion layer for NYC taxi data:
1. Create the payment_lookup seed asset from the CSV
2. Create the Python trips.py ingestion asset
Use append strategy, handle the taxi_types variable, fetch from TLC endpoint.
Reference: @pipeline/assets/ingestion/
```

**Layer 3 - Staging:**
```text
Build the staging layer to clean and deduplicate trips:
1. Create staging/trips.sql with time_interval strategy
2. Join with payment lookup, deduplicate using ROW_NUMBER
3. Add quality checks for required columns
Reference: @pipeline/assets/staging/
```

**Layer 4 - Reports:**
```text
Build the reports layer to aggregate data:
1. Create reports/trips_report.sql with time_interval strategy
2. Aggregate by date, taxi_type, payment_type
3. Add quality checks for the aggregated metrics
Reference: @pipeline/assets/reports/
```

**Full Pipeline Prompt**

If you prefer to build everything at once, use this comprehensive prompt:
```text
Build an end-to-end NYC Taxi data pipeline using Bruin.

Start with running `bruin init zoomcamp` to initialize the project.

## Context
- Project folder: @zoomcamp/pipeline
- Reference docs: @zoomcamp/README.md
- Use Bruin MCP tools for documentation lookup and command execution

## Instructions

### 1. Configuration (do this first)
- Create `.bruin.yml` with a DuckDB connection named `duckdb-default`
- Configure `pipeline.yml`: set name, schedule (monthly), start_date, default_connections, and the `taxi_types` variable (array of strings)

### 2. Build Assets (follow TODOs in each file)

NYC Taxi Raw Trip Source Details:
- **URL**: `https://d37ci6vzurychx.cloudfront.net/trip-data/`
- **Format**: Parquet files, one per taxi type per month
- **Naming**: `<taxi_type>_tripdata_<year>-<month>.parquet`
- **Examples**:
  - `yellow_tripdata_2022-03.parquet`
  - `green_tripdata_2025-01.parquet`
- **Taxi Types**: `yellow` (default), `green`

Build in this order, validating each with `bruin validate` before moving on:

a) **pipeline/assets/ingestion/payment_lookup.asset.yml** - Seed asset to load CSV lookup table
b) **pipeline/assets/ingestion/trips.py** - Python asset to fetch NYC taxi parquet data from TLC endpoint
   - Use `taxi_types` variable and date range from BRUIN_START_DATE/BRUIN_END_DATE
   - Add requirements.txt with: pandas, requests, pyarrow, python-dateutil
   - Keep the data in its rawest format without any cleaning or transformations
c) **pipeline/assets/staging/trips.sql** - SQL asset to clean, deduplicate (ROW_NUMBER), and enrich with payment lookup
   - Use `time_interval` strategy with `pickup_datetime` as incremental_key
d) **pipeline/assets/reports/trips_report.sql** - SQL asset to aggregate by date, taxi_type, payment_type
   - Use `time_interval` strategy for consistency

### 3. Validate & Run
- Validate entire pipeline: `bruin validate ./pipeline/pipeline.yml`
- Run with: `bruin run ./pipeline/pipeline.yml --full-refresh --start-date 2022-01-01 --end-date 2022-02-01`
- For faster testing, use `--var 'taxi_types=["yellow"]'` (skip green taxis)
- Note: Start with 1-3 months for development; run full backfill once complete

### 4. Verify Results
- Check row counts across all tables
- Query the reports table to confirm aggregations look correct
- Verify all quality checks passed (24 checks expected)
```

