# Homework: Batch Processing with Spark

## Questions & Answers

In this homework, we apply the Spark concepts from Module 6 on NYC Yellow Taxi data.

Main dataset:

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet
```

---

### Question 1: Install Spark and PySpark

- Install Spark
- Run PySpark
- Create a local Spark session
- Execute `spark.version`

What's the output?

Selected answer:

<p align="center">
  <img src="https://img.shields.io/badge/Answer-4.1.1-lightgreen" alt="Answer Q1">
</p>

---

### Question 2: Yellow November 2025

Read Yellow Taxi data for November 2025 into a Spark DataFrame.

Repartition the DataFrame into 4 partitions and save it as parquet.

What is the average parquet file size (files ending in `.parquet`, in MB)?

- 6MB
- 25MB
- 75MB
- 100MB

Selected answer:

```bash
ls -lah ./data/partitioned/yellow_2025_11_repartitioned
```

<p align="center">
  <img src="https://img.shields.io/badge/Answer-25MB-lightgreen" alt="Answer Q2">
</p>

---

### Question 3: Count Records

How many taxi trips were there on November 15?

Consider only trips that started on November 15.

- 62,610
- 102,340
- 162,604
- 225,768

Selected answer:

```sql
spark.sql("""
SELECT
    COUNT(1)
FROM 
    yellow_2025_11
WHERE
    to_date(tpep_pickup_datetime) = '2025-11-15';
""").show()
```

<p align="center">
  <img src="https://img.shields.io/badge/Answer-162,604-lightgreen" alt="Answer Q3">
</p>

---

### Question 4: Longest Trip

What is the length of the longest trip in the dataset (in hours)?

- 22.7
- 58.2
- 90.6
- 134.5

Selected answer:

```sql
spark.sql("""
SELECT
  ROUND(
    MAX((unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 3600.0),
    1
  ) AS longest_trip_hours
FROM yellow_2025_11
WHERE tpep_dropoff_datetime >= tpep_pickup_datetime
""").show()
```

<p align="center">
  <img src="https://img.shields.io/badge/Answer-90.6-lightgreen" alt="Answer Q4">
</p>

---

### Question 5: User Interface

Spark's User Interface dashboard runs on which local port?

- 80
- 443
- 4040
- 8080

Selected answer:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("spark-dashboard-demo")
    .config("spark.ui.enabled", "true")
    .config("spark.ui.port", "4040")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
)

print("Spark UI:", spark.sparkContext.uiWebUrl)

```

<p align="center">
  <img src="https://img.shields.io/badge/Answer-4040-lightgreen" alt="Answer Q5">
</p>

---

### Question 6: Least Frequent Pickup Location Zone

Load the zone lookup data into a temporary Spark view:

```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Using lookup data and Yellow November 2025 data, what is the **least frequent** pickup location zone?

- Governor's Island/Ellis Island/Liberty Island
- Arden Heights
- Rikers Island
- Jamaica Bay

Selected answer:

```sql
spark.sql("""
SELECT z.Zone, COUNT(*) AS trips
FROM yellow_2025_11 y
JOIN taxi_zone_lookup z
  ON y.PULocationID = z.LocationID
GROUP BY z.Zone
ORDER BY trips ASC, z.Zone ASC
LIMIT 10
""").show(truncate=False)
```

<p align="center">
  <img src="https://img.shields.io/badge/Answer-Arden Heights and Governor's Island/Ellis Island/Liberty Island-lightgreen" alt="Answer Q6">
</p>