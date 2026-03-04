# Minimal local Spark smoke test script.
import pyspark
from pyspark.sql import SparkSession

# Build a local Spark session to validate the runtime setup.
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

# Print the Spark version to confirm the session is active.
print(f"Spark version: {spark.version}")

# Create a tiny DataFrame and display it as a functional check.
df = spark.range(10)
df.show()

# Stop the session to release local resources.
spark.stop()
