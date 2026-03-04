#!/usr/bin/env python
# coding: utf-8

# # 04 Pyspark - Annotated Notebook
# 
# This notebook includes step-by-step markdown sections and English code comments.

# ## Step 1 - Import required libraries

# In[1]:


# Import required libraries.
import pyspark
from pyspark.sql import SparkSession


# ## Step 2 - Create a Spark session

# In[2]:


# Create a Spark session.
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()


# ## Step 3 - Download the input dataset

# In[3]:


# Download the input dataset.
get_ipython().system('wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-01.csv.gz')


# ## Step 4 - Inspect local files and raw data

# In[ ]:


# Inspect local files and raw data.
get_ipython().system('gzip -dc fhvhv_tripdata_2021-01.csv.gz')


# ## Step 5 - Inspect local files and raw data

# In[4]:


# Inspect local files and raw data.
get_ipython().system('wc -l fhvhv_tripdata_2021-01.csv')


# ## Step 6 - Load data into a Spark DataFrame

# In[5]:


# Load data into a Spark DataFrame.
df = spark.read \
    .option("header", "true") \
    .csv('fhvhv_tripdata_2021-01.csv')


# ## Step 7 - Inspect the DataFrame schema

# In[10]:


# Inspect the DataFrame schema.
df.schema


# ## Step 8 - Inspect local files and raw data

# In[14]:


# Inspect local files and raw data.
get_ipython().system('head -n 1001 fhvhv_tripdata_2021-01.csv > head.csv')


# ## Step 9 - Import required libraries

# In[15]:


# Import required libraries.
import pandas as pd


# ## Step 10 - Run the next processing step

# In[16]:


# Run the next processing step.
df_pandas = pd.read_csv('head.csv')


# ## Step 11 - Run the next processing step

# In[19]:


# Run the next processing step.
df_pandas.dtypes


# ## Step 12 - Inspect the DataFrame schema

# In[23]:


# Inspect the DataFrame schema.
spark.createDataFrame(df_pandas).schema


# Integer - 4 bytes
# Long - 8 bytes

# ## Step 13 - Import required libraries

# In[24]:


# Import required libraries.
from pyspark.sql import types


# ## Step 14 - Run the next processing step

# In[26]:


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


# ## Step 15 - Load data into a Spark DataFrame

# In[32]:


# Load data into a Spark DataFrame.
df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv('fhvhv_tripdata_2021-01.csv')


# ## Step 16 - Run the next processing step

# In[36]:


# Run the next processing step.
df = df.repartition(24)


# ## Step 17 - Write results to storage

# In[ ]:


# Write results to storage.
df.write.parquet('fhvhv/2021/01/')


# ## Step 18 - Load data into a Spark DataFrame

# In[44]:


# Load data into a Spark DataFrame.
df = spark.read.parquet('fhvhv/2021/01/')


# ## Step 19 - Inspect the DataFrame schema

# In[48]:


# Inspect the DataFrame schema.
df.printSchema()


# SELECT * FROM df WHERE hvfhs_license_num =  HV0003

# ## Step 20 - Import required libraries

# In[56]:


# Import required libraries.
from pyspark.sql import functions as F


# ## Step 21 - Inspect DataFrame content and size

# In[61]:


# Inspect DataFrame content and size.
df.show()


# ## Step 22 - Define a helper function

# In[63]:


# Define a helper function.
def crazy_stuff(base_num):
    num = int(base_num[1:])
    if num % 7 == 0:
        return f's/{num:03x}'
    elif num % 3 == 0:
        return f'a/{num:03x}'
    else:
        return f'e/{num:03x}'


# ## Step 23 - Run the next processing step

# In[65]:


# Run the next processing step.
crazy_stuff('B02884')


# ## Step 24 - Run the next processing step

# In[66]:


# Run the next processing step.
crazy_stuff_udf = F.udf(crazy_stuff, returnType=types.StringType())


# ## Step 25 - Run the next processing step

# In[67]:


# Run the next processing step.
df \
    .withColumn('pickup_date', F.to_date(df.pickup_datetime)) \
    .withColumn('dropoff_date', F.to_date(df.dropoff_datetime)) \
    .withColumn('base_id', crazy_stuff_udf(df.dispatching_base_num)) \
    .select('base_id', 'pickup_date', 'dropoff_date', 'PULocationID', 'DOLocationID') \
    .show()


# ## Step 26 - Apply DataFrame transformations

# In[55]:


# Apply DataFrame transformations.
df.select('pickup_datetime', 'dropoff_datetime', 'PULocationID', 'DOLocationID') \
  .filter(df.hvfhs_license_num == 'HV0003')


# ## Step 27 - Inspect local files and raw data

# In[50]:


# Inspect local files and raw data.
get_ipython().system('head -n 10 head.csv')


# ## Step 28 - Run this processing step

# In[ ]:


# Run this processing step.

