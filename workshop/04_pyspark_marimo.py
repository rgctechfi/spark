import marimo

__generated_with = "0.20.4"
app = marimo.App()


@app.cell(hide_code=True)
def _(mo):
    _df = mo.sql(
        f"""
        # marimo edit 04_pyspark_marimo.py
        """
    )
    return


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _():
    import subprocess

    return (subprocess,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # 04 Pyspark - Annotated Notebook

    This notebook includes step-by-step markdown sections and English code comments.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 1 - Import required libraries
    """)
    return


@app.cell
def _():
    # Import required libraries.
    import pyspark
    from pyspark.sql import SparkSession

    return (SparkSession,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 2 - Create a Spark session
    """)
    return


@app.cell
def _(SparkSession):
    # Create a Spark session.
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('test') \
        .getOrCreate()
    return (spark,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 3 - Download the input dataset
    """)
    return


@app.cell
def _(subprocess):
    # Download the input dataset.
    #! wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-01.csv.gz
    subprocess.call(['wget', 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-01.csv.gz'])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 4 - Inspect local files and raw data
    """)
    return


@app.cell
def _(subprocess):
    # Inspect local files and raw data.
    #! gzip -dc fhvhv_tripdata_2021-01.csv.gz
    subprocess.call(['gzip', '-dc', 'fhvhv_tripdata_2021-01.csv.gz'])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 5 - Inspect local files and raw data
    """)
    return


@app.cell
def _(subprocess):
    # Inspect local files and raw data.
    #! wc -l fhvhv_tripdata_2021-01.csv
    subprocess.call(['wc', '-l', 'fhvhv_tripdata_2021-01.csv'])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 6 - Load data into a Spark DataFrame
    """)
    return


@app.cell
def _(spark):
    # Load data into a Spark DataFrame.
    df = spark.read \
        .option("header", "true") \
        .csv('fhvhv_tripdata_2021-01.csv')
    return (df,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 7 - Inspect the DataFrame schema
    """)
    return


@app.cell
def _(df):
    # Inspect the DataFrame schema.
    df.schema
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 8 - Inspect local files and raw data
    """)
    return


@app.cell
def _(subprocess):
    # Inspect local files and raw data.
    #! head -n 1001 fhvhv_tripdata_2021-01.csv > head.csv
    with open("head.csv", "w", encoding="utf-8") as out:
        subprocess.run(
            ["head", "-n", "1001", "fhvhv_tripdata_2021-01.csv"],
            check=True,
            stdout=out,
        )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 9 - Import required libraries
    """)
    return


@app.cell
def _():
    # Import required libraries.
    import pandas as pd

    return (pd,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 10 - Run the next processing step
    """)
    return


@app.cell
def _(pd):
    # Run the next processing step.
    df_pandas = pd.read_csv('head.csv')
    return (df_pandas,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 11 - Run the next processing step
    """)
    return


@app.cell
def _(df_pandas):
    # Run the next processing step.
    df_pandas.dtypes
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 12 - Inspect the DataFrame schema
    """)
    return


@app.cell
def _(df_pandas, spark):
    # Inspect the DataFrame schema.
    spark.createDataFrame(df_pandas).schema
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    Integer - 4 bytes
    Long - 8 bytes
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 13 - Import required libraries
    """)
    return


@app.cell
def _():
    # Import required libraries.
    from pyspark.sql import types

    return (types,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 14 - Run the next processing step
    """)
    return


@app.cell
def _(types):
    # Run the next processing step.
    schema = types.StructType([
        types.StructField('hvfhs_license_num', types.StringType(), True),
        types.StructField('dispatching_base_num', types.StringType(), True),
        types.StructField('pickup_datetime', types.TimestampType(), True),
        types.StructField('dropoff_datetime', types.TimestampType(), True),
        types.StructField('PULocationID', types.IntegerType(), True),
        types.StructField('DOLocationID', types.IntegerType(), True),
        types.StructField('SR_Flag', types.StringType(), True)
    ])
    return (schema,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 15 - Load data into a Spark DataFrame
    """)
    return


@app.cell
def _(schema, spark):
    # Load data into a Spark DataFrame.
    df_1 = spark.read.option('header', 'true').schema(schema).csv('fhvhv_tripdata_2021-01.csv')
    return (df_1,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 16 - Run the next processing step
    """)
    return


@app.cell
def _(df_1):
    # Run the next processing step.
    df_2 = df_1.repartition(24)
    return (df_2,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 17 - Write results to storage
    """)
    return


@app.cell
def _(df_2):
    # Write results to storage.
    df_2.write.parquet('fhvhv/2021/01/')
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 18 - Load data into a Spark DataFrame
    """)
    return


@app.cell
def _(spark):
    # Load data into a Spark DataFrame.
    df_3 = spark.read.parquet('fhvhv/2021/01/')
    return (df_3,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 19 - Inspect the DataFrame schema
    """)
    return


@app.cell
def _(df_3):
    # Inspect the DataFrame schema.
    df_3.printSchema()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    SELECT * FROM df WHERE hvfhs_license_num =  HV0003
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 20 - Import required libraries
    """)
    return


@app.cell
def _():
    # Import required libraries.
    from pyspark.sql import functions as F

    return (F,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 21 - Inspect DataFrame content and size
    """)
    return


@app.cell
def _(df_3):
    # Inspect DataFrame content and size.
    df_3.show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 22 - Define a helper function
    """)
    return


@app.function
# Define a helper function.
def crazy_stuff(base_num):
    num = int(base_num[1:])
    if num % 7 == 0:
        return f's/{num:03x}'
    elif num % 3 == 0:
        return f'a/{num:03x}'
    else:
        return f'e/{num:03x}'


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 23 - Run the next processing step
    """)
    return


@app.cell
def _():
    # Run the next processing step.
    crazy_stuff('B02884')
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 24 - Run the next processing step
    """)
    return


@app.cell
def _(F, types):
    # Run the next processing step.
    crazy_stuff_udf = F.udf(crazy_stuff, returnType=types.StringType())
    return (crazy_stuff_udf,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 25 - Run the next processing step
    """)
    return


@app.cell
def _(F, crazy_stuff_udf, df_3):
    # Run the next processing step.
    df_3.withColumn('pickup_date', F.to_date(df_3.pickup_datetime)).withColumn('dropoff_date', F.to_date(df_3.dropoff_datetime)).withColumn('base_id', crazy_stuff_udf(df_3.dispatching_base_num)).select('base_id', 'pickup_date', 'dropoff_date', 'PULocationID', 'DOLocationID').show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 26 - Apply DataFrame transformations
    """)
    return


@app.cell
def _(df_3):
    # Apply DataFrame transformations.
    df_3.select('pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID').filter(df_3.hvfhs_license_num == 'HV0003')
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 27 - Inspect local files and raw data
    """)
    return


@app.cell
def _(subprocess):
    # Inspect local files and raw data.
    #! head -n 10 head.csv
    subprocess.call(['head', '-n', '10', 'head.csv'])
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Step 28 - Run this processing step
    """)
    return


if __name__ == "__main__":
    app.run()
