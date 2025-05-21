import os
import re
import sys
import datetime
import traceback
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect
from agno.agent import Agent
from agno.models.groq import Groq

# Load environment
load_dotenv()
groq_api_key = os.getenv("GROQ_API_KEY")
if not groq_api_key:
    raise ValueError("GROQ_API_KEY not set in .env")

# MySQL connection params
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASS = os.getenv("MYSQL_PASS", "hritesh12345")
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_DB   = os.getenv("MYSQL_DB",   "sales_db")

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

# Global variable to store the latest query result
latest_query_result = None
latest_query_columns = None

# --- 4. Define SQL Tools for AGNO Agent ---
def sql_db_list_tables(tool_input=None):
    """List all tables in the database.
    
    Args:
        tool_input: Optional input (not used for this function)
        
    Returns:
        String with comma-separated table names
    """
    tool_input = tool_input or ""  # Convert None to empty string
    try:
        tables = inspector.get_table_names()
        return ", ".join(tables)
    except Exception as e:
        traceback.print_exc()
        return f"Error listing tables: {str(e)}"

def sql_db_schema(table_names):
    """Given a comma-separated list of tables, returns the schema (DDL) and sample rows for those tables."""
    try:
        output = []
        table_names = [name.strip() for name in table_names.split(",") if name.strip()]
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
                    sample_str = ""
                    if sample:
                        colnames = [c["name"] for c in columns]
                        sample_str += "\t".join(colnames) + "\n"
                        for row in sample:
                            sample_str += "\t".join(str(val) for val in row) + "\n"
                    else:
                        sample_str += "(no rows)\n"
                output.append(f"{create_sql}\n/*\n{sample_str}*/")
            except Exception as e:
                traceback.print_exc()
                output.append(f"Error with table {table}: {str(e)}")
        return "\n\n".join(output)
    except Exception as e:
        traceback.print_exc()
        return f"General error with schema extraction: {str(e)}"

def sql_db_query_checker(query):
    """Double-checks if a SQL query is correct. Input: SQL query string. Output: the same query, or a corrected version if mistakes are found."""
    try:
        # This is a stub for LLM checking. In real use, the agent's LLM will check.
        # For now, just echo the query.
        return f"```sql\n{query}\n```"
    except Exception as e:
        return f"Error checking query: {str(e)}"

def sql_db_query(query):
    """Executes a SQL query and returns the results. Input: SQL query string. Output: result rows or error message."""
    global latest_query_result, latest_query_columns
    
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
            
            header = "\t".join(columns)
            out = [header]
            for row in rows:
                out.append("\t".join(str(val) for val in row))
            return "\n".join(out)
    except Exception as e:
        latest_query_result = None
        latest_query_columns = None
        traceback.print_exc()
        return f"Error executing query: {str(e)}"

def generate_plot(plot_type, x_column, y_column=None, title=None):
    """Generates a Plotly visualization based on the latest query results. 
    Supported plot_types: 'bar', 'pie'
    """
    global latest_query_result, latest_query_columns
    
    try:
        if latest_query_result is None or latest_query_columns is None:
            return "No query results available for visualization. Please run a query first."
        
        # Convert query results to DataFrame
        df = pd.DataFrame(latest_query_result, columns=latest_query_columns)
        
        # Check if specified columns exist
        if x_column not in df.columns:
            return f"Error: Column '{x_column}' not found in query results."
        
        if plot_type.lower() == 'bar' and y_column is not None:
            if y_column not in df.columns:
                return f"Error: Column '{y_column}' not found in query results."
        
        # Generate timestamp for file naming
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"results_{timestamp}.png"
        
        # Create appropriate visualization
        if plot_type.lower() == 'bar':
            if y_column:
                # Make sure y_column is numeric
                if pd.api.types.is_numeric_dtype(df[y_column]) or df[y_column].apply(lambda x: str(x).replace('.', '').isdigit()).all():
                    df[y_column] = pd.to_numeric(df[y_column], errors='coerce')
                    fig = px.bar(df, x=x_column, y=y_column, title=title or f"Bar Chart: {x_column} vs {y_column}")
                else:
                    # Count occurrences if y_column is not numeric
                    counts = df.groupby(x_column).size().reset_index(name='count')
                    fig = px.bar(counts, x=x_column, y='count', title=title or f"Bar Chart: Count of {x_column}")
            else:
                # Count occurrences if no y_column provided
                counts = df.groupby(x_column).size().reset_index(name='count')
                fig = px.bar(counts, x=x_column, y='count', title=title or f"Bar Chart: Count of {x_column}")
        
        elif plot_type.lower() == 'pie':
            if y_column and pd.api.types.is_numeric_dtype(df[y_column]) or df[y_column].apply(lambda x: str(x).replace('.', '').isdigit()).all():
                df[y_column] = pd.to_numeric(df[y_column], errors='coerce')
                # Group by x_column and sum y_column values
                grouped = df.groupby(x_column)[y_column].sum().reset_index()
                fig = px.pie(grouped, names=x_column, values=y_column, title=title or f"Pie Chart: {y_column} by {x_column}")
            else:
                # Count occurrences for pie chart
                counts = df[x_column].value_counts().reset_index()
                counts.columns = [x_column, 'count']
                fig = px.pie(counts, names=x_column, values='count', title=title or f"Pie Chart: Distribution of {x_column}")
        
        else:
            return f"Unsupported plot type: {plot_type}. Supported types are 'bar' and 'pie'."
        
        # Save the figure
        fig.write_image(filename)
        
        # Show the figure in a new window
        fig.show()
        
        # Return Plotly code for the user
        plotly_code = generate_plotly_code(plot_type, x_column, y_column, title)
        
        return f"Visualization created and saved as '{filename}'.\n\nHere's the Plotly code to recreate this chart:\n\n```python\n{plotly_code}```"
    
    except Exception as e:
        return f"Error generating visualization: {str(e)}"

def generate_plotly_code(plot_type, x_column, y_column=None, title=None):
    """Generates Python code for recreating the Plotly visualization."""
    
    code = [
        "import pandas as pd",
        "import plotly.express as px",
        "",
        "# Assuming df is your DataFrame with the query results",
        ""
    ]
    
    if plot_type.lower() == 'bar':
        if y_column:
            code.append(f"# Create bar chart of {x_column} vs {y_column}")
            code.append(f"fig = px.bar(df, x='{x_column}', y='{y_column}', title='{title or f'Bar Chart: {x_column} vs {y_column}'}')")
        else:
            code.append(f"# Count occurrences of {x_column}")
            code.append(f"counts = df.groupby('{x_column}').size().reset_index(name='count')")
            code.append(f"fig = px.bar(counts, x='{x_column}', y='count', title='{title or f'Bar Chart: Count of {x_column}'}')")
    
    elif plot_type.lower() == 'pie':
        if y_column:
            code.append(f"# Create pie chart showing {y_column} distribution by {x_column}")
            code.append(f"grouped = df.groupby('{x_column}')['{y_column}'].sum().reset_index()")
            code.append(f"fig = px.pie(grouped, names='{x_column}', values='{y_column}', title='{title or f'Pie Chart: {y_column} by {x_column}'}')")
        else:
            code.append(f"# Create pie chart showing distribution of {x_column}")
            code.append(f"counts = df['{x_column}'].value_counts().reset_index()")
            code.append(f"counts.columns = ['{x_column}', 'count']")
            code.append(f"fig = px.pie(counts, names='{x_column}', values='count', title='{title or f'Pie Chart: Distribution of {x_column}'}')")
    
    code.extend([
        "",
        "# Display the figure",
        "fig.show()",
        "",
        "# Save the figure",
        "fig.write_image('chart.png')"
    ])
    
    return "\n".join(code)

def plot_chart(plot_input):
    """Creates visualizations from the last query result using Plotly.
    Input format: 'plot_type|x_column|y_column(optional)|title(optional)'
    Example: 'bar|category|sales|Sales by Category'
    """
    try:
        parts = plot_input.split('|')
        if len(parts) < 2:
            return "Invalid input format. Expected: 'plot_type|x_column|y_column(optional)|title(optional)'"
        
        plot_type = parts[0].strip()
        x_column = parts[1].strip()
        y_column = parts[2].strip() if len(parts) > 2 and parts[2].strip() else None
        title = parts[3].strip() if len(parts) > 3 and parts[3].strip() else None
        
        return generate_plot(plot_type, x_column, y_column, title)
    except Exception as e:
        return f"Error processing plot request: {str(e)}"

# --- 5. Prepare the tools list ---
tools = [
    sql_db_list_tables,
    sql_db_schema,
    sql_db_query_checker,
    sql_db_query,
    plot_chart
]

# --- 6. Agent instructions ---
instructions = [
    "You are an agent designed to interact with a SQL database and create visualizations.",
    "Given an input question, always start by listing the tables in the database.",
    "Then, query the schema of the most relevant tables before writing any SQL query.",
    "When you generate a query, double-check it for correctness before executing.",
    "If you get an error executing a query, rewrite and retry.",
    "Unless the user specifies a specific number of examples, always LIMIT results to 5.",
    "Order results by a relevant column if possible to return the most interesting examples.",
    "Never SELECT * — only include relevant columns for the question.",
    "Do NOT make any DML statements (INSERT, UPDATE, DELETE, DROP, etc).",
    "Provide clear, concise answers based on the results.",
    "Show your reasoning and used SQL queries in your response.",
    "When the user asks for visualizations, use the plot_chart tool after executing a query.",
    "The plot_chart tool takes input in the format: 'plot_type|x_column|y_column(optional)|title(optional)'",
    "Supported plot types are 'bar' and 'pie'.",
    "For bar charts, x_column is the category and y_column is the value to plot.",
    "For pie charts, x_column is the category and y_column is the value to use for slice sizes.",
    "If no y_column is provided, the chart will count occurrences of each x_column value.",
    "Always suggest creating visualizations when appropriate based on the query results.",
]

# --- 7. Initialize and run the AGNO Agent ---
try:
    agent = Agent(
        model=Groq(id="llama-3.1-8b-instant", api_key=groq_api_key),
        tools=tools,
        instructions=instructions,
        markdown=True,
        show_tool_calls=True,
        add_datetime_to_instructions=True,
    )

    if __name__ == "__main__":
        try:
            print("SQL Agent with Visualization started. Type 'exit' or 'quit' to end.")
            while(True):
                question = input("Enter your question: ")
                if question.lower() in ['exit', 'quit']:
                    print("Exiting...")
                    break
                try:
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