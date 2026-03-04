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

```text
Pending
```

> [!NOTE]
> To install PySpark, follow this [guide](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/06-batch/setup/pyspark.md).

<p align="center">
  <img src="https://img.shields.io/badge/Answer-Pending-lightgrey" alt="Answer Q1">
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

```text
Pending
```

<p align="center">
  <img src="https://img.shields.io/badge/Answer-Pending-lightgrey" alt="Answer Q2">
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

```text
Pending
```

<p align="center">
  <img src="https://img.shields.io/badge/Answer-Pending-lightgrey" alt="Answer Q3">
</p>

---

### Question 4: Longest Trip

What is the length of the longest trip in the dataset (in hours)?

- 22.7
- 58.2
- 90.6
- 134.5

Selected answer:

```text
Pending
```

<p align="center">
  <img src="https://img.shields.io/badge/Answer-Pending-lightgrey" alt="Answer Q4">
</p>

---

### Question 5: User Interface

Spark's User Interface dashboard runs on which local port?

- 80
- 443
- 4040
- 8080

Selected answer:

```text
Pending
```

<p align="center">
  <img src="https://img.shields.io/badge/Answer-Pending-lightgrey" alt="Answer Q5">
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

```text
Pending
```

<p align="center">
  <img src="https://img.shields.io/badge/Answer-Pending-lightgrey" alt="Answer Q6">
</p>

---

## Submitting the Solutions

- Form for submitting: https://courses.datatalks.club/de-zoomcamp-2026/homework/hw6
- Deadline: See the website

## Learning in Public

We encourage everyone to share what they learned. This is called "learning in public".

Read more about the benefits [here](https://alexeyondata.substack.com/p/benefits-of-learning-in-public-and).

### Example Post for LinkedIn

```text
Week 6 of Data Engineering Zoomcamp by @DataTalksClub complete.

Just finished Module 6 - Batch Processing with Spark. Learned how to:

- Set up PySpark and create Spark sessions
- Read and process parquet files at scale
- Repartition data for better performance
- Analyze millions of taxi trips with DataFrames
- Use Spark UI for monitoring jobs

Here's my homework solution: <LINK>

You can sign up here: https://github.com/DataTalksClub/data-engineering-zoomcamp/
```

### Example Post for Twitter/X

```text
Module 6 of Data Engineering Zoomcamp done.

- Batch processing with Spark
- PySpark and DataFrames
- Parquet file optimization
- Spark UI on port 4040

My solution: <LINK>

Free course by @DataTalksClub: https://github.com/DataTalksClub/data-engineering-zoomcamp/
```
