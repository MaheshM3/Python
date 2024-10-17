import time
from databricks.connect import DatabricksSession

# Start timer
start_time = time.time()

# Create Databricks session using Databricks Connect
spark = DatabricksSession.builder.appName("Databricks Connect Test").getOrCreate()

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
print(f"Execution Time (Databricks Connect): {end_time - start_time} seconds")

# Stop the session
spark.stop()
