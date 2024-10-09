from concurrent.futures import ThreadPoolExecutor
import sqlparse
import re
from pyspark.sql import SparkSession
import pandas as pd

# Initialize Spark session
spark = SparkSession.builder.appName("OptimizedBatchQueries").getOrCreate()

# Path to data in ADLS Gen2
base_path = "abfss://<container>@<storage-account>.dfs.core.windows.net/<path-to-data>"

# Function to extract table names from SQL, including nested queries
def extract_table_names(query):
    parsed = sqlparse.parse(query)
    table_names = set()

    def extract_tables(tokens):
        from_seen = False
        for token in tokens:
            if token.is_keyword and token.value.upper() in ("FROM", "JOIN", ","):
                from_seen = True
            elif from_seen and token.ttype is None:
                if isinstance(token, sqlparse.sql.Identifier):
                    table_names.add(token.get_real_name())
                from_seen = False
            elif isinstance(token, sqlparse.sql.IdentifierList):
                extract_tables(token)
            elif isinstance(token, sqlparse.sql.Parenthesis):
                extract_tables(token.tokens)

    for statement in parsed:
        extract_tables(statement.tokens)

    return list(table_names)

# Function to extract partition filters from the SQL query
def extract_filters(query):
    business_date_match = re.search(r"business_date\s*=\s*'([\d\-]+)'", query, re.IGNORECASE)
    business_group_location_match = re.search(r"business_group_location\s*=\s*'(\w+)'", query, re.IGNORECASE)

    business_date = business_date_match.group(1) if business_date_match else None
    business_group_location = business_group_location_match.group(1) if business_group_location_match else None

    return {"business_date": business_date, "business_group_location": business_group_location}

# Function to load a table and register it as a temporary view
def load_table(tablename, filters):
    try:
        business_date = filters["business_date"]
        business_group_location = filters["business_group_location"]

        if business_date and business_group_location:
            table_path = f"{base_path}/{tablename}/business_date={business_date}/business_group_location={business_group_location}/"
        elif business_date:
            table_path = f"{base_path}/{tablename}/business_date={business_date}/"
        else:
            table_path = f"{base_path}/{tablename}/"

        df = spark.read.format("delta").option("mergeSchema", "true").load(table_path)
        df.createOrReplaceTempView(tablename)
        print(f"Table {tablename} loaded successfully.")
    except Exception as e:
        print(f"Error loading table {tablename}: {str(e)}")

# Function to run the queries after tables are loaded
def run_query(query, query_id, query_column):
    try:
        # Run the query
        result_df = spark.sql(query)
        result_value = result_df.collect()[0][0] if result_df.count() > 0 else 0
        return result_value
    except Exception as e:
        print(f"Error processing {query_column} for ID {query_id}: {str(e)}")
        return None

# Function to load tables in parallel
def load_all_tables(queries_df):
    # Extract all unique table names from the queries
    all_tables = set()
    for _, row in queries_df.iterrows():
        for query in [row['query1'], row['query2']]:
            tables = extract_table_names(query)
            all_tables.update(tables)

    # Extract partition filters (assumed same for all queries here)
    filters = extract_filters(queries_df.iloc[0]['query1'])  # Using filters from the first query for simplicity

    # Load each table in parallel
    with ThreadPoolExecutor(max_workers=4) as executor:
        for tablename in all_tables:
            executor.submit(load_table, tablename, filters)

# Function to run all queries in parallel after loading tables
def run_queries_parallel(queries_df, num_threads=4):
    results = []
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        for _, row in queries_df.iterrows():
            futures.append(executor.submit(run_query, row['query1'], row['ID'], 'query1'))
            futures.append(executor.submit(run_query, row['query2'], row['ID'], 'query2'))

        for future in futures:
            results.append(future.result())

    return results

# Read queries CSV file uploaded in Databricks
queries_csv_path = "/dbfs/FileStore/tables/queries.csv"  # Path to the CSV file in Databricks workspace
queries_df = spark.read.csv(queries_csv_path, header=True).toPandas()

# Load all tables first
load_all_tables(queries_df)

# Run the queries in parallel after tables are loaded
results = run_queries_parallel(queries_df, num_threads=4)

# Reformat results back into dataframe
output_data = []
for idx, row in queries_df.iterrows():
    query1_count = results[idx * 2]    # First result for this ID
    query2_count = results[idx * 2 + 1]  # Second result for this ID
    output_data.append({
        "ID": row['ID'],
        "query1": row['query1'],
        "query2": row['query2'],
        "query1Count": query1_count,
        "query2Count": query2_count
    })

# Create a dataframe for the output
output_df = pd.DataFrame(output_data)

# Output dataframe example
print(output_df)

# Stop Spark session after queries are done
spark.stop()
