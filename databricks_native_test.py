import time
from pyspark.sql import SparkSession

# Start timer
start_time = time.time()

# Create Spark session natively on the Databricks cluster
spark = SparkSession.builder \
    .appName("Databricks Native Test") \
    .getOrCreate()

# Load your data (replace the path with actual data path in your cluster)
df = spark.read.format("delta").load("/path/to/your/data")

# Perform some transformations (replace with relevant columns)
df_transformed = df.groupBy("some_column").agg({"another_column": "sum"})

# Show execution plan (for comparison)
df_transformed.explain(True)

# Trigger an action to force execution
df_transformed.collect()

# End timer
end_time = time.time()

# Print total execution time
print(f"Execution Time (Native SparkSession): {end_time - start_time} seconds")

# Stop the session
spark.stop()
