import sqlparse
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML

def extract_tables_from_query(query):
    """
    Function to extract all table names from a given SQL query.
    
    Parameters:
        query (str): The SQL query as a string.
        
    Returns:
        List[str]: A list of all table names used in the query.
    """
    
    # Parse the SQL query
    parsed_query = sqlparse.parse(query)
    
    # To store all the table names found
    tables = set()

    # Helper function to extract table names from Identifier or IdentifierList
    def extract_from_token(token):
        if isinstance(token, IdentifierList):
            for identifier in token.get_identifiers():
                tables.add(str(identifier.get_real_name()))
        elif isinstance(token, Identifier):
            tables.add(str(token.get_real_name()))
        elif token.ttype is Keyword and token.value.upper() == 'FROM':
            # If we hit a "FROM" clause, it should be followed by table names
            idx = token.parent.token_index(token) + 1
            next_token = token.parent.tokens[idx]
            extract_from_token(next_token)

    # Iterate over the parsed query statements
    for stmt in parsed_query:
        # Process each token within the SQL statement
        from_seen = False
        for token in stmt.tokens:
            if token.is_whitespace:
                continue
            
            # Look for DML statements that introduce tables, e.g., SELECT, INSERT, UPDATE, DELETE
            if token.ttype is DML and token.value.upper() in ('SELECT', 'INSERT', 'UPDATE', 'DELETE'):
                from_seen = True

            if from_seen:
                # If we have seen a DML statement, search for tables in FROM/JOIN clauses, etc.
                extract_from_token(token)

            # Special case for Common Table Expressions (WITH clause)
            if isinstance(token, Identifier) and token.get_real_name() is not None:
                # If the token is an Identifier (possibly a table alias)
                tables.add(token.get_real_name())
            
            if token.ttype is Keyword and token.value.upper() in ('JOIN', 'FROM', 'INTO'):
                # Handle JOIN, FROM, INTO keywords followed by table names
                extract_from_token(token)

    return list(tables)

# Example usage:
query = """
    WITH temp AS (
        SELECT id, name FROM users
    )
    SELECT a.id, b.order_id
    FROM accounts a
    JOIN orders b ON a.id = b.account_id
    WHERE a.status = 'active'
"""
tables = extract_tables_from_query(query)
print("Tables used in query:", tables)
