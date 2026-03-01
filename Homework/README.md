# Homework: Build Your Own dlt Pipeline

## Questions & Answers

Once your pipeline has run successfully, use the methods covered in the workshop to investigate the following questions.

### Question 1: What is the start date and end date of the dataset?

- 2009-01-01 to 2009-01-31
- 2009-06-01 to 2009-07-01
- 2024-01-01 to 2024-02-01
- 2024-06-01 to 2024-07-01

Selected answer:

```sql
SELECT MIN(trip_pickup_date_time), 
MAX(trip_pickup_date_time) 
FROM taxi_dataset.taxi_trip")
```

<p align="center">
  <img src="https://img.shields.io/static/v1?label=Answer&message=2009-06-01%20to%202009-07-01&color=darkgreen" alt="Answer Q1">
</p>

---

### Question 2: What proportion of trips are paid with credit card?

- 16.66%
- 26.66%
- 36.66%
- 46.66%

Selected answer:

```sql
WITH trip_stats AS (
    SELECT 
        COUNT(*) as total_trips,
        SUM(CASE WHEN payment_type = 'Credit' THEN 1 ELSE 0 END) as credit_trips
    FROM taxi_dataset.taxi_trip
)
SELECT 
    total_trips,
    credit_trips,
    ROUND(100.0 * credit_trips / total_trips, 2) as proportion_percent
FROM trip_stats
```

<p align="center">
  <img src="https://img.shields.io/badge/Answer-26.66%25-darkgreen" alt="Answer Q2">
</p>

---

### Question 3: What is the total amount of money generated in tips?

- $4,063.41
- $6,063.41
- $8,063.41
- $10,063.41

Selected answer:

<p align="center">
  <img src="./ressources/tips_amt.png" alt="tips amount" width="400" />
  <br/>
  <img src="./ressources/all_amt.png" alt="all amount" width="400" />
  <br/>
  <img src="./ressources/nb_row.png" alt="row count" width="400" />
</p>

```text
10 841 is for total_amount not total_tips !
```

<p align="center">
  <img src="https://img.shields.io/badge/Answer-553-darkgreen" alt="Answer Q3">
</p>