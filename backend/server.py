import os
import re
import sys
import traceback
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect
from agno.agent import Agent
from agno.models.groq import Groq
import seaborn as sns
import matplotlib.pyplot as plt
import json

# Load environment
load_dotenv()
groq_api_key = os.getenv("GROQ_API_KEY")
if not groq_api_key:
    raise ValueError("GROQ_API_KEY not set in .env")

# MySQL connection params
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASS = os.getenv("MYSQL_PASS")
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT")
MYSQL_DB   = os.getenv("MYSQL_DB")

print(f"Connecting to MySQL: {MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB} as {MYSQL_USER}")

# Create MySQL engine
try:
    engine = create_engine(
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
    )
    
    # Test the connection
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
        print("MySQL connection successful!")
except Exception as e:
    print(f"MySQL connection error: {str(e)}")
    print("Attempting to use SQLite instead...")
    
    # Fall back to SQLite if MySQL connection fails
    try:
        sqlite_path = os.path.join(os.path.dirname(__file__), "Chinook.db")
        print(f"Looking for SQLite database at: {sqlite_path}")
        
        if os.path.exists(sqlite_path):
            engine = create_engine(f"sqlite:///{sqlite_path}")
            print("SQLite connection successful!")
        else:
            print(f"ERROR: SQLite database file not found at {sqlite_path}")
            sys.exit(1)
    except Exception as e:
        print(f"SQLite connection error: {str(e)}")
        sys.exit(1)

# Inspector for database metadata
try:
    inspector = inspect(engine)
    print(f"Database metadata inspection initialized")
except Exception as e:
    print(f"Error initializing inspector: {str(e)}")
    traceback.print_exc()
    sys.exit(1)

# Global variables
latest_query_result = None
latest_query_columns = None
database_context = None  # Will store complete DB schema and sample data

# --- Database Context Creation Functions ---

def build_database_context():
    """Build comprehensive database context with schema and sample data for all tables."""
    global database_context
    
    try:
        tables = inspector.get_table_names()
        context_parts = []
        
        for table in tables:
            try:
                # Get schema
                columns = inspector.get_columns(table)
                col_lines = [
                    f"{col['name']} {col['type']}{' PRIMARY KEY' if col.get('primary_key') else ''}"
                    for col in columns
                ]
                create_sql = f"CREATE TABLE {table} (\n    " + ",\n    ".join(col_lines) + "\n)"
                
                # Get sample rows
                with engine.connect() as conn:
                    sample = conn.execute(text(f"SELECT * FROM {table} LIMIT 3")).fetchall()
                    sample_str = ""
                    if sample:
                        colnames = [c["name"] for c in columns]
                        sample_str += "SAMPLE DATA:\n"
                        sample_str += "\t".join(colnames) + "\n"
                        for row in sample:
                            sample_str += "\t".join(str(val) for val in row) + "\n"
                    else:
                        sample_str += "(no data in table)\n"
                
                context_parts.append(f"TABLE: {table}\n{create_sql}\n{sample_str}")
            except Exception as e:
                print(f"Error building context for table {table}: {str(e)}")
                context_parts.append(f"TABLE: {table} - Error retrieving schema: {str(e)}")
        
        database_context = "\n\n".join(context_parts)
        print(f"Database context built successfully with {len(tables)} tables")
    except Exception as e:
        print(f"Error building database context: {str(e)}")
        database_context = f"Error building database context: {str(e)}"

# Build context on startup
build_database_context()

# --- 4. Define SQL Tools for AGNO Agent ---

def sql_db_list_tables(tool_input=None):
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

def sql_db_schema(table_names):
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

def sql_db_query_checker(query):
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

def sql_db_query(query):
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

def create_pie_chart(data_input):
    """Create and save a pie chart as PNG using seaborn/matplotlib."""
    global latest_query_result, latest_query_columns
    if not latest_query_result or not latest_query_columns:
        return "Error: No query results available. Please run a SQL query first."
    try:
        cfg = json.loads(data_input)
        title = cfg.get('title', 'Pie Chart')
        labels_col = cfg.get('labels_column')
        values_col = cfg.get('values_column')
        if not labels_col or not values_col:
            return f"Error: 'labels_column' and 'values_column' required. Available: {', '.join(latest_query_columns)}"
        idx_l = list(latest_query_columns).index(labels_col)
        idx_v = list(latest_query_columns).index(values_col)
        labels = [row[idx_l] for row in latest_query_result]
        vals = [float(row[idx_v]) for row in latest_query_result]
        plt.figure(figsize=(8,6))
        plt.pie(vals, labels=labels, autopct='%1.1f%%')
        plt.title(title)
        png = 'pie_chart.png'
        plt.savefig(png)
        plt.close()
        return f"Pie chart saved as '{png}'."
    except Exception as e:
        return f"Error creating pie chart: {str(e)}"

def create_bar_chart(data_input):
    """Create and save a bar chart as PNG using seaborn/matplotlib."""
    global latest_query_result, latest_query_columns
    try:
        cfg = json.loads(data_input)
        title = cfg.get('title', 'Bar Chart')
        # If direct arrays provided, use them
        if 'labels' in cfg and 'values' in cfg:
            labels = cfg['labels']
            vals = cfg['values']
            plt.figure(figsize=(10,6))
            sns.barplot(x=labels, y=vals)
            plt.xlabel('')
            plt.ylabel('')
            plt.title(title)
            png = 'bar_chart.png'
            plt.savefig(png)
            plt.close()
            return f"Bar chart saved as '{png}' using provided data."
        # Otherwise use latest query results
        if not latest_query_result or not latest_query_columns:
            return "Error: No query results available. Please run a SQL query first."
        x_col = cfg.get('x_column')
        y_col = cfg.get('y_column')
        if not x_col or not y_col:
            return f"Error: 'x_column' and 'y_column' required. Available: {', '.join(latest_query_columns)}"
        idx_x = list(latest_query_columns).index(x_col)
        idx_y = list(latest_query_columns).index(y_col)
        x = [row[idx_x] for row in latest_query_result]
        y = [float(row[idx_y]) for row in latest_query_result]
        plt.figure(figsize=(10,6))
        sns.barplot(x=x, y=y)
        plt.xlabel(x_col)
        plt.ylabel(y_col)
        plt.title(title)
        png = 'bar_chart.png'
        plt.savefig(png)
        plt.close()
        return f"Bar chart saved as '{png}' using query results."
    except Exception as e:
        return f"Error creating bar chart: {str(e)}"

def get_db_capabilities():
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

# --- 5. Prepare the tools list ---
tools = [
    sql_db_list_tables,
    sql_db_schema,
    sql_db_query_checker,
    sql_db_query,
    get_db_capabilities,
    create_pie_chart,
    create_bar_chart
]

# --- 6. Agent instructions ---
instructions = [
     "You are an agent designed to interact with a SQL database and create visualizations.",
        "When given a question, follow these steps:",
        "1. ALWAYS start by listing the tables in the database using sql_db_list_tables.",
        "2. ALWAYS check schema of relevant tables using sql_db_schema before writing any SQL.",
        "3. Write SQL queries that relate ONLY to tables and columns that actually exist in the database.",
        "4. Validate your SQL query using sql_db_query_checker before executing.",
        "5. Execute the query with sql_db_query."
        "",
        "CRITICAL RULES:",
        "- NEVER mention or use tables that don't exist in the database.",
        "- ALWAYS use table names EXACTLY as they appear in sql_db_list_tables.",
        "- Use ONLY column names that appear in the table schemas.",
        "- NEVER make up column names or table names that don't exist.",
        "- Unless the user specifies a specific number of examples, always LIMIT results to 5.",
        "- Order results by a relevant column if possible to return the most interesting examples.",
        "- Never SELECT * — only include relevant columns for the question.",
        "- Do NOT make any DML statements (INSERT, UPDATE, DELETE, DROP, etc).",
        "- If unsure about database functions or syntax, use get_db_capabilities to check what's supported.",
        "",        "Visualization capabilities:",
        "- Use create_pie_chart with a JSON configuration to create pie charts from query results.",
        "- Use create_bar_chart with a JSON configuration to create bar charts from query results or direct data arrays.",
        "- For pie charts, provide 'title', 'labels_column', and 'values_column' in the JSON configuration.",
        "- For bar charts using query results, provide 'title', 'x_column', and 'y_column' in the JSON configuration.",
        "- For bar charts with direct data, provide 'title', 'labels' (array), and 'values' (array) in the JSON configuration.",
        "- Always run a SQL query first before creating visualizations from query results."
        "",
        "Error handling:",
        "- If a table or column doesn't exist, clearly explain which ones are invalid.",
        "- If a query fails, analyze the error and suggest corrections.",
        "- If the user request is beyond the capability of the database, explain why and suggest alternatives.",
        "",
        "Return format: ",
        "-The reasoning or the thought process behind the query.",
        "-A small summary in 2 lines about the query result.",
        "-The output from the SQL query in a table format as shown below",
        "-It may be possible to create a visualization from the query result. If so, just say image has been generated",
        "-There may be a case where all of these are not possible, but try to generate as much as similar to the return format as possible.",
        """
        Here is a sample output format:

        <Result from SQL>
        +----------------+-------------+
        | category       | total_sales |
        +----------------+-------------+
        | Books          |     6229990 |
        | Clothing       |     5745085 |
        | Smartphones    |     4149426 |
        | Laptops        |     3175717 |
        | Home & Kitchen |     2027246 |
        +----------------+-------------+
        """
        f"Complete database context: {database_context}"
]

# --- 7. Initialize and run the AGNO Agent ---
try:
    agent = Agent(
        model=Groq(id="llama-3.3-70b-versatile", api_key=groq_api_key),
        tools=tools,
        instructions=instructions,
        reasoning=True,
        markdown=True,
        show_tool_calls=True,
        add_datetime_to_instructions=True,
    )

    if __name__ == "__main__":
        try:
            print("SQL Agent with Visualization started. Type 'exit' or 'quit' to end.")
            print(f"Connected to database type: {engine.name}")
            
            while True:
                question = input("Enter your question: ")
                if question.lower() in ['exit', 'quit']:
                    print("Exiting...")
                    break
                try:
                    # Always refresh database context before answering
                    build_database_context()
                    
                    # Update instructions with latest database context
                    context_instructions = instructions.copy()
                    context_instructions[-1] = f"Complete database context: {database_context}"

                    # Re-initialize the Agent with the new context
                    agent = Agent(
                        model=Groq(id="llama-3.3-70b-versatile", api_key=groq_api_key),
                        tools=tools,
                        instructions=context_instructions,
                        reasoning=True,
                        markdown=True,
                        show_tool_calls=True,
                        add_datetime_to_instructions=True,
                    )

                    agent.print_response(question, stream=True)
                except Exception as e:
                    print(f"Error during agent response: {str(e)}")
                    traceback.print_exc()
        except Exception as e:
            print(f"Error in main loop: {str(e)}")
            traceback.print_exc()
except Exception as e:
    print(f"Error initializing agent: {str(e)}")
    traceback.print_exc()