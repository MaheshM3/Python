from pyspark.sql import SparkSession
from concurrent.futures import ThreadPoolExecutor
import sqlparse
import re

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

# Function to load a table, register it as a temporary view, and apply partition filters
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

# Function to execute a query and return the result count
def run_query(query, query_id, query_column):
    try:
        tablenames = extract_table_names(query)
        if not tablenames:
            raise ValueError(f"No table names found in query: {query}")

        filters = extract_filters(query)
        for tablename in tablenames:
            load_table(tablename, filters)

        result_df = spark.sql(query)
        result_value = result_df.collect()[0][0] if result_df.count() > 0 else 0

        return result_value

    except Exception as e:
        print(f"Error processing {query_column} for ID {query_id}: {str(e)}")
        return None

# Function to run queries in parallel using ThreadPoolExecutor
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

# Example of a small dataframe for testing
import pandas as pd

data = {
    'ID': ['1', '2'],
    'query1': [
        "SELECT COUNT(*) FROM t1 WHERE business_date = '2024-01-01' AND business_group_location = 'US'",
        "SELECT COUNT(*) FROM t2 WHERE business_date = '2024-01-02' AND business_group_location = 'EU'"
    ],
    'query2': [
        "SELECT COUNT(*) FROM t3 WHERE business_date = '2024-01-01' AND business_group_location = 'US'",
        "SELECT COUNT(*) FROM t4 WHERE business_date = '2024-01-02' AND business_group_location = 'EU'"
    ]
}

queries_df = pd.DataFrame(data)

# Run the queries in parallel
results = run_queries_parallel(queries_df, num_threads=4)

# Stop Spark session after queries are done
spark.stop()
