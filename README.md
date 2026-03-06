<h1 align="center">
  <span>𝘼𝙥𝙖𝙘𝙝𝙚 𝙎𝙥𝙖𝙧𝙠</span>
</h1>

<p align="center">
  <img src="./ressources/pictures/spark_logo.jpg" alt="Spark image" width="240" />
</p>

<p align="center">
  <em>Batch processing with PySpark: local setup, SQL/DataFrames, cloud execution, and homework deliverables</em>
</p>

## Overview

This repository consolidates Module 6 (Batch Processing with Spark) work:

- annotated workshop notebooks (`03` to `09`)
- executable Spark scripts for local and cloud runs
- a full project/homework notebook on NYC Yellow Taxi (November 2025)
- a project summary of validated answers and queries

## Environment

From `pyproject.toml`:

- Python `>= 3.13`
- `pyspark >= 4.1.1`
- `jupyter >= 1.1.1`
- `marimo >= 0.20.4`

## Quick Start

```bash
uv sync
source .venv/bin/activate
jupyter notebook
```

Minimal Spark smoke test:

```bash
python workshop/test_spark.py
```

## Data Sources Used

- Yellow Taxi November 2025 parquet:
  - `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet`
- Taxi zone lookup CSV:
  - `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`
- FHVHV January 2021 (workshop):
  - `https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-01.csv.gz`

Download safely to `./data`:

```bash
mkdir -p ./data
if [ ! -f ./data/yellow_tripdata_2025-11.parquet ]; then
  wget -P ./data https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet
fi
if [ ! -f ./data/taxi_zone_lookup.csv ]; then
  wget -P ./data https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
fi
```

## Repository Map

```text
.
├── README.md
├── pyproject.toml
├── main.py
├── ressources/
│   └── pictures/spark_logo.jpg
├── workshop/
│   ├── 03_test.ipynb
│   ├── 04_pyspark.ipynb
│   ├── 04_pyspark_Ipython.py
│   ├── 04_pyspark_marimo.py
│   ├── 05_taxi_schema.ipynb
│   ├── 06_spark_sql.ipynb
│   ├── 06_spark_sql.py
│   ├── 06_spark_sql_big_query.py
│   ├── 07_groupby_join.ipynb
│   ├── 08_rdds.ipynb
│   ├── 09_spark_gcs.ipynb
│   ├── cloud.md
│   ├── download_data.sh
│   ├── test_spark.py
│   └── homework.ipynb
└── project/
    ├── README.md
    ├── homework.ipynb
    └── data/
```

## Workshop Track

### Notebooks

- `03_test.ipynb`: local Spark quick validation (CSV -> Spark -> Parquet)
- `04_pyspark.ipynb`: foundational DataFrame workflow and UDF introduction
- `05_taxi_schema.ipynb`: explicit schema design and type handling
- `06_spark_sql.ipynb`: SQL/DataFrame transformations and monthly aggregations
- `07_groupby_join.ipynb`: grouping and joins
- `08_rdds.ipynb`: RDD transformations and distributed concepts
- `09_spark_gcs.ipynb`: Spark + Google Cloud Storage integration
- `workshop/homework.ipynb`: annotated homework practice notebook

### Scripts

- `workshop/test_spark.py`: local runtime smoke test (`spark.version`, simple DataFrame)
- `workshop/download_data.sh`: yearly/monthly raw taxi download helper
- `workshop/06_spark_sql.py`:
  - normalizes green/yellow schemas
  - unions both datasets
  - computes monthly revenue KPIs
  - writes parquet output
- `workshop/06_spark_sql_big_query.py`:
  - same KPI pipeline
  - writes result to BigQuery (with temporary GCS bucket config)

### Cloud Notes

`workshop/cloud.md` documents:

- standalone Spark cluster setup (`start-master`, `start-worker`)
- `spark-submit` flow
- Dataproc job submission
- BigQuery connector usage

## Project Track (Yellow Taxi November 2025)

The notebook in `project/homework.ipynb` follows a structured implementation flow for Questions 1 to 6.  
The focus is on building a reproducible Spark workflow, not only producing final values.

Technical development logic:

1. Initialize runtime: import PySpark modules, create `SparkSession`, and verify session state/version.
2. Ingest source data: read `yellow_tripdata_2025-11.parquet` into a DataFrame and inspect schema/sample rows.
3. Register SQL context: create temp views (e.g. `yellow_2025_11`) to switch easily between SQL and DataFrame APIs.
4. Persist partitioned output: `repartition(4)` then write parquet to a target folder for file-size and partition analysis.
5. Validate storage artifacts: inspect generated `part-*` parquet files and `_SUCCESS` markers from Spark writes.
6. Implement date-based analytics: derive pickup date (`to_date`) and run count aggregations for daily filtering use cases.
7. Implement trip-duration analytics: compute duration from pickup/dropoff timestamps with safe timestamp-to-seconds conversion.
8. Enable observability: trigger actions and inspect Spark UI (`uiWebUrl`) for stages, jobs, and task execution behavior.
9. Enrich with lookup data: load `taxi_zone_lookup.csv`, align key types (`LocationID`), and join on `PULocationID`.
10. Rank low-frequency pickup zones: aggregate by `Zone`, sort ascending by trip count, and limit output for answer candidates.
11. Cross-check logic in two styles: keep equivalent SQL and PySpark DataFrame versions for clarity and debugging.
12. Handle notebook edge cases: address path overwrite conflicts and Spark session reconnect issues before reruns.

## Useful Operational Notes

- Spark UI can auto-increment ports if busy (`4040`, `4041`, ...).
- For repeated writes to the same parquet path, use overwrite mode:

```python
df.repartition(4).write.mode("overwrite").parquet("partitioned/yellow_2025_11_repartitioned")
```

- `ls` size units:
  - file sizes are bytes in plain `ls -l`
  - `total` (macOS/BSD) is in 512-byte blocks
  - use `ls -lah` / `du -sh` for human-readable output

- If a Spark session gets unstable in notebooks (`ConnectionRefusedError`), restart kernel and recreate the session before continuing.

## Related Files

- Project summary and final homework Q/A: `project/README.md`
- Cloud run cookbook: `workshop/cloud.md`
