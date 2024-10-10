from concurrent.futures import ThreadPoolExecutor

# Load the queries from a CSV file using spark.read.csv
# Assuming the CSV file has two columns: id and query
queries_df = spark.read.csv("queries.csv", header=True, inferSchema=True)

# Function to execute SQL query and return id, query, and the result count
def run_query(row):
    query_id = row['id']
    query = row['query']
    
    # Execute the query (which is SELECT COUNT(*)) and fetch the count
    query_result = spark.sql(query).collect()
    
    # Extract the count from the query result (first row, first column)
    query_count = query_result[0][0] if query_result else 0
    
    return (query_id, query, query_count)

# Convert the DataFrame rows to a list of dictionaries for easier mapping
queries_list = queries_df.collect()

# Execute the queries in parallel
with ThreadPoolExecutor(max_workers=3) as executor:
    results = list(executor.map(run_query, queries_list))

# Process the results
for result in results:
    query_id, query, query_count = result
    print(f"ID: {query_id}, Query: {query}, Row Count: {query_count}")
