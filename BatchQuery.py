from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from concurrent.futures import ThreadPoolExecutor
import re

# Initialize Spark session
spark = SparkSession.builder.appName("BatchQueries").getOrCreate()

# List of queries (replace with actual queries)
queries_list = [
    {"query": "SELECT * FROM table1 WHERE region = 'US' AND date = '2024-01-01'"},
    {"query": "SELECT * FROM table2 WHERE region = 'EU' AND date = '2024-01-02'"},
    # Add more queries as needed
]

# Path to data in ADLS Gen2
base_path = "abfss://<container>@<storage-account>.dfs.core.windows.net/<path-to-data>"

# Function to extract table name from the SQL query
def extract_table_name(query):
    # A basic regex to extract table name (you might need to refine this based on your query structure)
    match = re.search(r'FROM\s+(\w+)', query, re.IGNORECASE)
    if match:
        return match.group(1)
    return None

# Function to extract filter conditions from the SQL query (e.g., partition info like date, region)
def extract_filters(query):
    # Look for specific filter patterns in WHERE clause (e.g., region, date)
    date_match = re.search(r"date\s*=\s*'([\d\-]+)'", query, re.IGNORECASE)
    region_match = re.search(r"region\s*=\s*'(\w+)'", query, re.IGNORECASE)

    date = date_match.group(1) if date_match else None
    region = region_match.group(1) if region_match else None

    return {"date": date, "region": region}

# Function to load data for a specific table and run a query
def run_query(query_dict):
    try:
        query = query_dict["query"]

        # Extract table name from the query
        tablename = extract_table_name(query)
        if not tablename:
            raise ValueError(f"Table name not found in query: {query}")

        # Extract partition filters (e.g., date, region)
        filters = extract_filters(query)
        date = filters.get("date")
        region = filters.get("region")

        # Construct the path dynamically based on the table, date, and region
        # Assuming date and region are partition columns
        if date and region:
            table_path = f"{base_path}/{tablename}/date={date}/region={region}/"
        elif date:
            table_path = f"{base_path}/{tablename}/date={date}/"
        else:
            table_path = f"{base_path}/{tablename}/"

        # Read data from the specific partition, use partition pruning
        df = spark.read.option("mergeSchema", "true").parquet(table_path)

        # Register as a temporary table for SQL querying
        df.createOrReplaceTempView(tablename)

        # Run the query
        result_df = spark.sql(query)

        # Show results or write to output (optional)
        result_df.show()  # You can write to storage instead of displaying

    except Exception as e:
        print(f"Error processing query: {query}. Error: {str(e)}")

# Set up thread pool for parallel query execution
num_threads = 10  # Adjust based on the cluster's resource capabilities
with ThreadPoolExecutor(max_workers=num_threads) as executor:
    # Submit queries to be run in parallel
    futures = [executor.submit(run_query, query) for query in queries_list]

    # Wait for all queries to complete
    for future in futures:
        future.result()

# Stop the Spark session after all queries are complete
spark.stop()
