import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML

# Simulated schema information: table names as keys and column names as values
table_schemas = {
    'users': ['id', 'name', 'email', 'age'],
    'accounts': ['id', 'status', 'created_at', 'balance'],
    'orders': ['order_id', 'account_id', 'total', 'order_date'],
    'transactions': ['transaction_id', 'account_id', 'amount', 'transaction_date'],
    # Add more tables and their columns as needed
}

def extract_tables_and_aliases_from_query(query):
    """
    Extracts all table names and aliases from a given SQL query.
    
    Parameters:
        query (str): The SQL query.
        
    Returns:
        Dict[str, str]: A dictionary mapping aliases to their corresponding table names.
    """
    parsed_query = sqlparse.parse(query)
    tables_and_aliases = {}

    def extract_from_token(token):
        if isinstance(token, IdentifierList):
            for identifier in token.get_identifiers():
                extract_from_token(identifier)
        elif isinstance(token, Identifier):
            real_name = str(token.get_real_name()).lower()
            alias = str(token.get_alias()).lower() if token.get_alias() else real_name
            tables_and_aliases[alias] = real_name
        elif token.ttype is Keyword and token.value.upper() == 'FROM':
            idx = token.parent.token_index(token) + 1
            next_token = token.parent.tokens[idx]
            extract_from_token(next_token)

    for stmt in parsed_query:
        from_seen = False
        for token in stmt.tokens:
            if token.is_whitespace:
                continue
            if token.ttype is DML and token.value.upper() in ('SELECT', 'INSERT', 'UPDATE', 'DELETE'):
                from_seen = True
            if from_seen:
                extract_from_token(token)
            if isinstance(token, Identifier) and token.get_real_name() is not None:
                real_name = token.get_real_name().lower()
                alias = token.get_alias().lower() if token.get_alias() else real_name
                tables_and_aliases[alias] = real_name
            if token.ttype is Keyword and token.value.upper() in ('JOIN', 'FROM', 'INTO'):
                extract_from_token(token)

    return tables_and_aliases

def find_filter_columns_in_query(query, filter_columns):
    """
    Extracts all filter columns used in the query, considering possible table aliases.
    
    Parameters:
        query (str): The SQL query.
        filter_columns (List[str]): List of filter columns to check.
        
    Returns:
        Dict[str, List[str]]: A dictionary mapping table names to the filter columns found.
    """
    parsed_query = sqlparse.parse(query)
    filter_columns = [col.lower() for col in filter_columns]  # Normalize filter columns
    columns_in_query = {}

    def extract_columns(token):
        if isinstance(token, Identifier):
            real_column = token.get_real_name().lower()  # Column name
            table_alias = token.get_parent_name().lower() if token.get_parent_name() else None  # Alias
            if real_column in filter_columns:
                return table_alias, real_column
        return None, None

    for stmt in parsed_query:
        for token in stmt.tokens:
            if isinstance(token, IdentifierList):
                for identifier in token.get_identifiers():
                    alias, column = extract_columns(identifier)
                    if alias and column:
                        if alias not in columns_in_query:
                            columns_in_query[alias] = []
                        columns_in_query[alias].append(column)
            elif isinstance(token, Identifier):
                alias, column = extract_columns(token)
                if alias and column:
                    if alias not in columns_in_query:
                        columns_in_query[alias] = []
                    columns_in_query[alias].append(column)

    return columns_in_query

def filter_tables_by_columns(tables_and_aliases, columns_in_query, table_schemas):
    """
    Filters tables to check which ones contain the specified filter columns.
    
    Parameters:
        tables_and_aliases (Dict[str, str]): A dictionary mapping table aliases to actual table names.
        columns_in_query (Dict[str, List[str]]): Columns used in the query, by alias.
        table_schemas (Dict[str, List[str]]): The schema dictionary mapping table names to their columns.
        
    Returns:
        Dict[str, List[str]]: A dictionary where the keys are table names and the values are the filter columns found in those tables.
    """
    result = {}

    for alias, columns in columns_in_query.items():
        table_name = tables_and_aliases.get(alias)
        if table_name in table_schemas:
            # Find common columns between query columns and table columns (case-insensitive)
            common_columns = set(columns).intersection([col.lower() for col in table_schemas[table_name]])
            if common_columns:
                result[table_name] = list(common_columns)
    
    return result


# Example SQL query with aliases and mixed case column names
query = """
    WITH temp AS (
        SELECT id, name FROM users
    )
    SELECT a.id, b.order_id, a.STATUS
    FROM Accounts a
    JOIN Orders b ON a.ID = b.ACCOUNT_id
    WHERE a.Status = 'active' AND b.ORDER_DATE > '2023-01-01'
"""

# Step 1: Extract tables and aliases from the query
tables_and_aliases = extract_tables_and_aliases_from_query(query)
print("Tables and aliases used in query:", tables_and_aliases)

# Step 2: Extract filter columns used in the query (even with aliases)
filter_columns = ['status', 'order_date', 'balance']
columns_in_query = find_filter_columns_in_query(query, filter_columns)
print("Columns used in query with table aliases:", columns_in_query)

# Step 3: Find which tables contain the filter columns, considering aliases
tables_with_columns = filter_tables_by_columns(tables_and_aliases, columns_in_query, table_schemas)
print("Tables with specified filter columns:")
for table, columns in tables_with_columns.items():
    print(f"{table}: {columns}")
