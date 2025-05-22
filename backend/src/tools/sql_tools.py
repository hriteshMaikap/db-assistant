"""
SQL query tools for the database assistant.
Provides functions for listing tables, getting schema, and executing queries.
"""
import re
import traceback
from sqlalchemy import text

# Global variables to store query results
latest_query_result = None
latest_query_columns = None

def sql_db_list_tables(inspector, tool_input=None):
    """List all tables in the database.
    
    This function returns all available tables in the connected database.
    No input is required for this function.
    
    Returns:
        String with comma-separated table names
    """
    try:
        tables = inspector.get_table_names()
        if not tables:
            return "No tables found in database."
        return ", ".join(tables)
    except Exception as e:
        error_msg = f"Error listing tables: {str(e)}"
        print(error_msg)
        traceback.print_exc()
        return error_msg

def sql_db_schema(engine, inspector, table_names):
    """Get schema and sample data for specified tables.
    
    Args:
        table_names: A comma-separated string of table names (e.g., "customers,orders")
    
    Returns:
        String containing table schemas with column names, data types, 
        and sample data (first 3 rows) for each specified table
    """
    try:
        output = []
        table_names = [name.strip() for name in table_names.split(",") if name.strip()]
        all_tables = inspector.get_table_names()
        
        # Check if tables exist
        invalid_tables = [t for t in table_names if t not in all_tables]
        if invalid_tables:
            return f"Error: The following tables do not exist: {', '.join(invalid_tables)}. Available tables are: {', '.join(all_tables)}"
        
        for table in table_names:
            try:
                # Schema
                columns = inspector.get_columns(table)
                col_lines = [
                    f"{col['name']} {col['type']}{' PRIMARY KEY' if col.get('primary_key') else ''}"
                    for col in columns
                ]
                create_sql = f"CREATE TABLE {table} (\n    " + ",\n    ".join(col_lines) + "\n)"

                # Sample rows
                with engine.connect() as conn:
                    sample = conn.execute(text(f"SELECT * FROM {table} LIMIT 3")).fetchall()
                    sample_str = "SAMPLE DATA:\n" if sample else "(no rows)\n"
                    if sample:
                        colnames = [c["name"] for c in columns]
                        sample_str += "\t".join(colnames) + "\n"
                        for row in sample:
                            sample_str += "\t".join(str(val) for val in row) + "\n"
                
                output.append(f"TABLE: {table}\n{create_sql}\n{sample_str}")
            except Exception as e:
                error_msg = f"Error with table {table}: {str(e)}"
                print(error_msg)
                traceback.print_exc()
                output.append(error_msg)
        
        return "\n\n".join(output)
    except Exception as e:
        error_msg = f"General error with schema extraction: {str(e)}"
        print(error_msg)
        traceback.print_exc()
        return error_msg

def sql_db_query_checker(engine, inspector, query):
    """Double-check and validate SQL query before execution.
    
    This function checks if the SQL query is valid, safe, and formatted correctly.
    It helps prevent errors and ensures the query will run successfully.
    
    Args:
        query: The SQL query string to validate
    
    Returns:
        Either the validated query or a corrected version if issues were found
    """
    # Validate query is not empty
    if not query or not query.strip():
        return "Error: Empty query provided"
    
    # Check for dangerous operations
    dangerous_patterns = [
        r'\bDROP\b',
        r'\bDELETE\b',
        r'\bTRUNCATE\b',
        r'\bUPDATE\b',
        r'\bINSERT\b',
        r'\bALTER\b',
        r'\bCREATE\b',
        r'\bEXEC\b'
    ]
    
    for pattern in dangerous_patterns:
        if re.search(pattern, query, re.IGNORECASE):
            return f"Error: Potentially dangerous operation detected ({pattern.strip('\\b')}). Only SELECT statements are permitted."
    
    # Verify database functions in query match the database engine
    db_type = engine.name
    
    if db_type == 'sqlite':
        # Check for MySQL/other DB specific functions in SQLite
        invalid_functions = [
            (r'\bYEAR\s*\(', 'Use strftime("%Y", date_column) instead of YEAR()'),
            (r'\bMONTH\s*\(', 'Use strftime("%m", date_column) instead of MONTH()'),
            (r'\bDAY\s*\(', 'Use strftime("%d", date_column) instead of DAY()'),
        ]
        
        for pattern, suggestion in invalid_functions:
            if re.search(pattern, query, re.IGNORECASE):
                return f"Error: Function not supported in SQLite. {suggestion}"
    
    # Ensure all tables mentioned exist in the database
    tables = inspector.get_table_names()
    
    # Extract table names from FROM and JOIN clauses (basic extraction, not perfect)
    query_tables = re.findall(r'(?:FROM|JOIN)\s+([a-zA-Z0-9_]+)', query, re.IGNORECASE)
    missing_tables = [table for table in query_tables if table not in tables]
    
    if missing_tables:
        return f"Error: The following tables do not exist: {', '.join(missing_tables)}. Available tables are: {', '.join(tables)}"
    
    # For demonstration only - actual LLM would perform more complex checks
    # and potentially fix the query
    return f"```sql\n{query}\n```"

def sql_db_query(engine, inspector, query):
    """Execute SQL query and return results.
    
    This function runs the SQL query against the database and returns the results.
    Only SELECT statements are permitted.
    
    Args:
        query: The SQL query string to execute
    
    Returns:
        Formatted results from the query execution or error message
    """
    global latest_query_result, latest_query_columns
    
    # Validate it's a SELECT query
    if not query.strip().upper().startswith('SELECT'):
        return "Error: Only SELECT queries are allowed. Please provide a SELECT statement."
    
    try:
        with engine.connect() as conn:
            print(f"Executing query: {query}")
            result = conn.execute(text(query))
            rows = result.fetchall()
            
            if not rows:
                latest_query_result = None
                latest_query_columns = None
                return "(no rows returned)"
            
            # Store column names
            if hasattr(rows[0], "_fields"):
                columns = rows[0]._fields
            else:
                columns = result.keys() if hasattr(result, 'keys') else [str(i) for i in range(len(rows[0]))]
            
            # Store results and column names for visualization
            latest_query_result = rows
            latest_query_columns = columns
            
            # Format output - using markdown table format for better readability
            header = "| " + " | ".join(str(col) for col in columns) + " |"
            separator = "| " + " | ".join(["---"] * len(columns)) + " |"
            
            out = [header, separator]
            for row in rows:
                out.append("| " + " | ".join(str(val).replace("|", "\\|") for val in row) + " |")
            
            return "\n".join(out)
    except Exception as e:
        latest_query_result = None
        latest_query_columns = None
        error_msg = f"Error executing query: {str(e)}"
        print(error_msg)
        traceback.print_exc()
        
        # Extract table names from the query
        tables = re.findall(r'(?:FROM|JOIN)\s+([a-zA-Z0-9_]+)', query, re.IGNORECASE)
        available_tables = inspector.get_table_names()
        
        # Provide helpful error context
        context = f"\n\nAvailable tables: {', '.join(available_tables)}"
        if tables:
            # Check if the tables mentioned exist
            missing_tables = [t for t in tables if t not in available_tables]
            if missing_tables:
                context += f"\n\nError details: Tables {', '.join(missing_tables)} do not exist."
        
        return error_msg + context

def get_db_capabilities(engine):
    """Get information about the database capabilities and limitations.
    
    This function returns details about the database type, supported functions,
    and any specific syntax considerations.
    
    Returns:
        String with database capability information
    """
    db_type = engine.name
    
    if db_type == 'sqlite':
        return """
Database Type: SQLite

Key SQLite Capabilities:
1. Date/Time Functions: Use strftime() instead of YEAR(), MONTH(), DAY()
   - Example: strftime('%Y', date_column) instead of YEAR(date_column)
   - Format codes: %Y (year), %m (month), %d (day), %H (hour), %M (minute), %S (second)

2. Aggregation Functions: 
   - COUNT, SUM, AVG, MIN, MAX
   - GROUP_CONCAT (similar to MySQL's GROUP_CONCAT)

3. Limitations:
   - No stored procedures
   - Limited date arithmetic
   - No native JSON functions
   - No window functions in older versions
   
4. Query Examples:
   - Basic date extraction: SELECT strftime('%Y', date_column) as year, COUNT(*) FROM table GROUP BY year
   - Substring: SELECT substr(column, start_pos, length) FROM table
"""
    elif db_type == 'mysql':
        return """
Database Type: MySQL

Key MySQL Capabilities:
1. Date/Time Functions: 
   - YEAR(), MONTH(), DAY(), HOUR(), MINUTE(), SECOND()
   - DATE(), TIME(), NOW(), CURDATE()

2. String Functions:
   - CONCAT(), SUBSTRING(), LENGTH(), UPPER(), LOWER()

3. Advanced Features:
   - JSON functions
   - Window functions
   - Stored procedures

4. Query Examples:
   - Date extraction: SELECT YEAR(date_column) as year, COUNT(*) FROM table GROUP BY year
   - String concatenation: SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM customers
"""
    else:
        return f"""
Database Type: {db_type}

Generic SQL capabilities available:
- Basic queries with SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY
- Aggregation functions: COUNT, SUM, AVG, MIN, MAX
- Joins: INNER JOIN, LEFT JOIN, RIGHT JOIN
"""

# Get the latest query results for visualization
def get_latest_query_results():
    return latest_query_result, latest_query_columns
