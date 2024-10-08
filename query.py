import sqlparse
from pyspark.sql import SparkSession
import re

# Initialize Spark session
spark = SparkSession.builder.appName("BatchQueries").getOrCreate()

# Path to data in ADLS Gen2
base_path = "abfss://<container>@<storage-account>.dfs.core.windows.net/<path-to-data>"

# Function to extract table names from SQL, including nested queries
def extract_table_names(query):
    parsed = sqlparse.parse(query)
    table_names = set()

    # Recursive function to walk through parsed SQL tokens
    def extract_tables(tokens):
        from_seen = False
        for token in tokens:
            # If token is a 'FROM', 'JOIN', or ',', expect a table name to follow
            if token.is_keyword and token.value.upper() in ("FROM", "JOIN", ","):
                from_seen = True
            elif from_seen and token.ttype is None:  # Expecting table name
                if isinstance(token, sqlparse.sql.Identifier):
                    table_names.add(token.get_real_name())
                from_seen = False
            elif isinstance(token, sqlparse.sql.IdentifierList):
                extract_tables(token)  # Recurse through identifier list
            elif isinstance(token, sqlparse.sql.Parenthesis):
                extract_tables(token.tokens)  # Handle nested queries or subqueries

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
        # Extract all table names from the query, including nested queries
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

# Example query from the user
query = """
SELECT COUNT(*)
FROM t1 
JOIN (
    SELECT * 
    FROM (
        SELECT * 
        FROM t2, t3
    )
);
"""

# Example of running the query
filters = extract_filters(query)
tablenames = extract_table_names(query)

for tablename in tablenames:
    load_table(tablename, filters)

# Run the query and get the result
result = run_query(query, query_id="example", query_column="example_query")
print(f"Query result: {result}")

# Stop the Spark session after all queries are complete
spark.stop()
