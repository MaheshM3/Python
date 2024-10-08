from pyspark.sql import SparkSession
import re

# Initialize Spark session
spark = SparkSession.builder.appName("BatchQueries").getOrCreate()

# Path to the CSV file containing the queries (in DBFS or ADLS)
csv_file_path = "/dbfs/mnt/<mount-point>/queries.csv"  # Change to your path

# Read the CSV file into a DataFrame
queries_df = spark.read.format("csv").option("header", "true").load(csv_file_path)

# Path to data in ADLS Gen2
base_path = "abfss://<container>@<storage-account>.dfs.core.windows.net/<path-to-data>"

# Function to extract all table names from the SQL query (handling implicit and explicit joins)
def extract_table_names(query):
    # Regex to capture tables in both explicit and implicit joins
    table_pattern = re.compile(r'FROM\s+([\w]+)\s*(?:,\s*([\w]+))?', re.IGNORECASE)
    
    # Find tables in the query
    tables = []
    for match in table_pattern.findall(query):
        tables += [t for t in match if t]  # Collect non-empty matches

    return list(set(tables))  # Return unique table names

# Function to extract partition filters from the SQL query (e.g., business_date, business_group_location)
def extract_filters(query):
    business_date_match = re.search(r"business_date\s*=\s*'([\d\-]+)'", query, re.IGNORECASE)
    business_group_location_match = re.search(r"business_group_location\s*=\s*'(\w+)'", query, re.IGNORECASE)

    business_date = business_date_match.group(1) if business_date_match else None
    business_group_location = business_group_location_match.group(1) if business_group_location_match else None

    return {"business_date": business_date, "business_group_location": business_group_location}

# Function to load a table, register it as a temporary view, and apply partition filters
def load_table(tablename, filters):
    try:
        # Extract filters
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
        print(f"Table {tablename} loaded successfully.")
    except Exception as e:
        print(f"Error loading table {tablename}: {str(e)}")

# Function to execute a query and return the result count
def run_query(query, query_id, query_column):
    try:
        # Extract all table names from the query, including implicit and explicit joins
        tablenames = extract_table_names(query)
        if not tablenames:
            raise ValueError(f"No table names found in query: {query}")

        # Extract partition filters (e.g., business_date, business_group_location)
        filters = extract_filters(query)

        # Load each table and register it as a temporary view
        for tablename in tablenames:
            load_table(tablename, filters)

        # Run the query
        result_df = spark.sql(query)

        # Extract the result value (assuming it's a count or a single value result)
        result_value = result_df.collect()[0][0]

        # Return the result value
        return result_value

    except Exception as e:
        print(f"Error processing {query_column} for ID {query_id}: {str(e)}")
        return None

# Initialize an empty list to hold the results
results = []

# Run each query sequentially
for query_row in queries_df.collect():
    query_dict = query_row.asDict()
    query_id = query_dict["id"]

    # Extract the queries
    query1 = query_dict["query1"]
    query2 = query_dict["query2"]

    # Run query1 and get the result count
    query1_count = run_query(query1, query_id, "query1")

    # Run query2 and get the result count
    query2_count = run_query(query2, query_id, "query2")

    # Append the result as a tuple (ID, query1, query2, query1Count, query2Count)
    results.append((query_id, query1, query2, query1_count, query2_count))

# Define the schema explicitly to avoid type inference issues
from pyspark.sql.types import StructType, StructField, StringType, LongType

schema = StructType([
    StructField("ID", StringType(), True),
    StructField("query1", StringType(), True),
    StructField("query2", StringType(), True),
    StructField("query1Count", LongType(), True),  # Assuming the count is a long integer
    StructField("query2Count", LongType(), True)   # Assuming the count is a long integer
])

# Convert the results into a DataFrame with the defined schema
result_df = spark.createDataFrame(results, schema)

# Show the results
result_df.show()

# Stop the Spark session after all queries are complete
spark.stop()
