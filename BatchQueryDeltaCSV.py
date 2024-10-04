from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from concurrent.futures import ThreadPoolExecutor
import re

# Initialize Spark session
spark = SparkSession.builder.appName("BatchQueries").getOrCreate()

# Path to the CSV file containing the queries (in DBFS or ADLS)
csv_file_path = "/dbfs/mnt/<mount-point>/queries.csv"  # Change to your path

# Read the CSV file into a DataFrame
queries_df = spark.read.format("csv").option("header", "true").load(csv_file_path)

# Path to data in ADLS Gen2
base_path = "abfss://<container>@<storage-account>.dfs.core.windows.net/<path-to-data>"

# Function to extract table name from the SQL query
def extract_table_name(query):
    match = re.search(r'FROM\s+(\w+)', query, re.IGNORECASE)
    if match:
        return match.group(1)
    return None

# Function to extract partition filters from the SQL query (e.g., business_date, business_group_location)
def extract_filters(query):
    business_date_match = re.search(r"business_date\s*=\s*'([\d\-]+)'", query, re.IGNORECASE)
    business_group_location_match = re.search(r"business_group_location\s*=\s*'(\w+)'", query, re.IGNORECASE)

    business_date = business_date_match.group(1) if business_date_match else None
    business_group_location = business_group_location_match.group(1) if business_group_location_match else None

    return {"business_date": business_date, "business_group_location": business_group_location}

# Function to load data for a specific table and run a query
def run_query(query_dict):
    try:
        query = query_dict["query"]

        # Extract table name from the query
        tablename = extract_table_name(query)
        if not tablename:
            raise ValueError(f"Table name not found in query: {query}")

        # Extract partition filters (e.g., business_date, business_group_location)
        filters = extract_filters(query)
        business_date = filters["business_date"]
        business_group_location = filters["business_group_location"]

        # Construct the path dynamically based on the table, business_date, and business_group_location
        if business_date and business_group_location:
            table_path = f"{base_path}/{tablename}/business_date={business_date}/business_group_location={business_group_location}/"
        elif business_date:
            table_path = f"{base_path}/{tablename}/business_date={business_date}/"
        else:
            table_path = f"{base_path}/{tablename}/"

        # Read data from the specific partition using Delta format
        df = spark.read.format("delta").option("mergeSchema", "true").load(table_path)

        # Register as a temporary table for SQL querying
        df.createOrReplaceTempView(tablename)

        # Run the query
        result_df = spark.sql(query)

        # Show results or write to output (optional)
        result_df.show()  # You can write to storage instead of displaying

    except Exception as e:
        print(f"Error processing query: {query}. Error: {str(e)}")

# Collect the query information from the DataFrame into a list of dictionaries
queries_list = queries_df.collect()

# Set up thread pool for parallel query execution
num_threads = 10  # Adjust based on the cluster's resource capabilities
with ThreadPoolExecutor(max_workers=num_threads) as executor:
    # Submit queries to be run in parallel
    futures = [executor.submit(run_query, query.asDict()) for query in queries_list]

    # Wait for all queries to complete
    for future in futures:
        future.result()

# Stop the Spark session after all queries are complete
spark.stop()
