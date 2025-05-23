import os
import re
import sys
import json
import datetime
import traceback
import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient
from bson import ObjectId, json_util
from sqlalchemy import create_engine, text, inspect
from typing import Dict, List, Any, Optional
from agno.agent import Agent
from agno.models.groq import Groq
import matplotlib.pyplot as plt
import seaborn as sns

# Load environment variables
load_dotenv()
groq_api_key = os.getenv("GROQ_API_KEY")
if not groq_api_key:
    raise ValueError("GROQ_API_KEY not set in .env")

# Global variables
latest_query_result = None
latest_query_columns = None
mongodb_client = None
mongodb_db = None
sql_engine = None
sql_inspector = None
database_context = None

# --- MongoDB Setup ---
def initialize_mongodb():
    """Initialize MongoDB connection"""
    global mongodb_client, mongodb_db
    
    MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
    MONGODB_DB_NAME = os.getenv("MONGODB_DB_NAME", "university_management")
    
    try:
        mongodb_client = MongoClient(MONGODB_URI)
        mongodb_db = mongodb_client[MONGODB_DB_NAME]
        # Test connection
        mongodb_client.admin.command('ping')
        print(f"MongoDB connection successful: {MONGODB_URI}/{MONGODB_DB_NAME}")
        return True
    except Exception as e:
        print(f"MongoDB connection failed: {str(e)}")
        return False

# --- SQL Setup ---
def initialize_sql():
    """Initialize SQL connection (MySQL with SQLite fallback)"""
    global sql_engine, sql_inspector
    
    # Try MySQL first
    MYSQL_USER = os.getenv("MYSQL_USER")
    MYSQL_PASS = os.getenv("MYSQL_PASS")
    MYSQL_HOST = os.getenv("MYSQL_HOST")
    MYSQL_PORT = os.getenv("MYSQL_PORT")
    MYSQL_DB = os.getenv("MYSQL_DB")
    
    if all([MYSQL_USER, MYSQL_PASS, MYSQL_HOST, MYSQL_PORT, MYSQL_DB]):
        try:
            sql_engine = create_engine(
                f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
            )
            with sql_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            sql_inspector = inspect(sql_engine)
            print(f"MySQL connection successful: {MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}")
            return True
        except Exception as e:
            print(f"MySQL connection failed: {str(e)}")
    
    # Fall back to SQLite
    try:
        sqlite_path = os.path.join(os.path.dirname(__file__), "Chinook.db")
        if os.path.exists(sqlite_path):
            sql_engine = create_engine(f"sqlite:///{sqlite_path}")
            sql_inspector = inspect(sql_engine)
            print(f"SQLite connection successful: {sqlite_path}")
            return True
        else:
            print(f"SQLite database not found at: {sqlite_path}")
            return False
    except Exception as e:
        print(f"SQLite connection failed: {str(e)}")
        return False

# Initialize connections
mongodb_available = initialize_mongodb()
sql_available = initialize_sql()

if not mongodb_available and not sql_available:
    print("ERROR: No database connections available!")
    sys.exit(1)

print(f"Available databases: MongoDB={mongodb_available}, SQL={sql_available}")

# --- MongoDB Tools ---
class MongoJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)

def mongo_db_list_collections(tool_input=None):
    """List all collections in the MongoDB database."""
    if not mongodb_available:
        return "Error: MongoDB is not available"
    
    try:
        collections = mongodb_db.list_collection_names()
        return f"MongoDB Collections: {', '.join(collections)}" if collections else "No MongoDB collections found"
    except Exception as e:
        return f"Error listing MongoDB collections: {str(e)}"

def mongo_db_schema(tool_input: str):
    """Get schema and sample documents for MongoDB collections."""
    if not mongodb_available:
        return "Error: MongoDB is not available"
    
    try:
        collections = [name.strip() for name in tool_input.split(",") if name.strip()]
        output = []
        
        for collection_name in collections:
            try:
                collection = mongodb_db[collection_name]
                sample_size = int(os.getenv("MONGODB_SCHEMA_SAMPLE_SIZE", "100"))
                documents = list(collection.find().limit(sample_size))
                
                if not documents:
                    output.append(f"## MongoDB Collection: {collection_name}\nNo documents found")
                    continue
                
                # Infer schema
                schema = {}
                for doc in documents:
                    for field, value in doc.items():
                        if field == '_id':
                            continue
                        field_type = type(value).__name__
                        if field not in schema:
                            schema[field] = {"type": field_type, "example": value}
                        elif schema[field]["type"] != field_type:
                            if isinstance(schema[field]["type"], list):
                                if field_type not in schema[field]["type"]:
                                    schema[field]["type"].append(field_type)
                            else:
                                schema[field]["type"] = [schema[field]["type"], field_type]
                
                # Format output
                schema_md = f"## MongoDB Collection: {collection_name}\n\n"
                schema_md += f"Document count: {len(documents)}\n\n"
                schema_md += "### Schema:\n| Field | Type | Example |\n|-------|------|--------|\n"
                
                for field, info in schema.items():
                    field_type = info["type"]
                    if isinstance(field_type, list):
                        field_type = " or ".join(field_type)
                    
                    example = info["example"]
                    if isinstance(example, (dict, list)):
                        example_str = "complex structure"
                    else:
                        example_str = str(example)[:30] + ("..." if len(str(example)) > 30 else "")
                    
                    schema_md += f"| {field} | {field_type} | {example_str} |\n"
                
                schema_md += f"\n### Sample Document:\n```json\n{json.dumps(documents[0], indent=2, cls=MongoJSONEncoder)}\n```\n"
                output.append(schema_md)
                
            except Exception as e:
                output.append(f"Error with MongoDB collection {collection_name}: {str(e)}")
        
        return "\n\n".join(output)
    except Exception as e:
        return f"Error with MongoDB schema extraction: {str(e)}"

def mongo_db_query(query_input):
    """Execute MongoDB query (find or aggregate) and return results."""
    if not mongodb_available:
        return "Error: MongoDB is not available"
    
    global latest_query_result, latest_query_columns
    
    try:
        query_params = json.loads(query_input)
        collection_name = query_params.get("collection")
        
        if not collection_name:
            return "Error: Collection name is required"
        
        collection = mongodb_db[collection_name]
        pipeline = query_params.get("pipeline")
        
        if pipeline:
            # Aggregation query
            documents = list(collection.aggregate(pipeline))
        else:
            # Find query
            filter_query = query_params.get("filter", {})
            projection = query_params.get("projection")
            sort = query_params.get("sort")
            skip = query_params.get("skip", 0)
            limit = query_params.get("limit", 5)
            
            cursor = collection.find(filter_query, projection)
            if sort:
                cursor = cursor.sort(sort)
            cursor = cursor.skip(skip)
            if limit > 0:
                cursor = cursor.limit(limit)
            
            documents = list(cursor)
        
        if not documents:
            latest_query_result = None
            latest_query_columns = None
            return "(no MongoDB documents returned)"
        
        # Format results as table
        all_fields = set()
        for doc in documents:
            all_fields.update(doc.keys())
        
        if not pipeline and '_id' in all_fields and (not query_params.get("projection") or '_id' not in query_params.get("projection", {})):
            all_fields.remove('_id')
        
        columns = sorted(list(all_fields))
        latest_query_result = documents
        latest_query_columns = columns
        
        # Create table format
        header = "| " + " | ".join(columns) + " |"
        separator = "| " + " | ".join(["---"] * len(columns)) + " |"
        
        rows = [header, separator]
        for doc in documents:
            row_values = []
            for col in columns:
                val = doc.get(col, "")
                if isinstance(val, (dict, list)):
                    val = json.dumps(val, cls=MongoJSONEncoder)
                elif isinstance(val, ObjectId):
                    val = str(val)
                row_values.append(str(val).replace("|", "\\|"))
            rows.append("| " + " | ".join(row_values) + " |")
        
        return "\n".join(rows)
    
    except json.JSONDecodeError:
        return f"Error: Invalid JSON in MongoDB query: {query_input}"
    except Exception as e:
        latest_query_result = None
        latest_query_columns = None
        return f"Error executing MongoDB query: {str(e)}"

def mongo_count_documents(collection_name, filter_query=None):
    """Count documents in MongoDB collection."""
    if not mongodb_available:
        return "Error: MongoDB is not available"
    
    try:
        collection = mongodb_db[collection_name]
        filter_query = json.loads(filter_query) if filter_query else {}
        count = collection.count_documents(filter_query)
        return f"MongoDB document count: {count}"
    except Exception as e:
        return f"Error counting MongoDB documents: {str(e)}"

# --- SQL Tools ---
def sql_db_list_tables(tool_input=None):
    """List all tables in the SQL database."""
    if not sql_available:
        return "Error: SQL database is not available"
    
    try:
        tables = sql_inspector.get_table_names()
        return f"SQL Tables: {', '.join(tables)}" if tables else "No SQL tables found"
    except Exception as e:
        return f"Error listing SQL tables: {str(e)}"

def sql_db_schema(table_names):
    """Get schema and sample data for SQL tables."""
    if not sql_available:
        return "Error: SQL database is not available"
    
    try:
        output = []
        table_names = [name.strip() for name in table_names.split(",") if name.strip()]
        all_tables = sql_inspector.get_table_names()
        
        invalid_tables = [t for t in table_names if t not in all_tables]
        if invalid_tables:
            return f"Error: SQL tables do not exist: {', '.join(invalid_tables)}. Available: {', '.join(all_tables)}"
        
        for table in table_names:
            try:
                columns = sql_inspector.get_columns(table)
                col_lines = [
                    f"{col['name']} {col['type']}{' PRIMARY KEY' if col.get('primary_key') else ''}"
                    for col in columns
                ]
                create_sql = f"CREATE TABLE {table} (\n    " + ",\n    ".join(col_lines) + "\n)"
                
                with sql_engine.connect() as conn:
                    sample = conn.execute(text(f"SELECT * FROM {table} LIMIT 3")).fetchall()
                    sample_str = "SAMPLE DATA:\n" if sample else "(no rows)\n"
                    if sample:
                        colnames = [c["name"] for c in columns]
                        sample_str += "\t".join(colnames) + "\n"
                        for row in sample:
                            sample_str += "\t".join(str(val) for val in row) + "\n"
                
                output.append(f"## SQL TABLE: {table}\n{create_sql}\n{sample_str}")
            except Exception as e:
                output.append(f"Error with SQL table {table}: {str(e)}")
        
        return "\n\n".join(output)
    except Exception as e:
        return f"Error with SQL schema extraction: {str(e)}"

def sql_db_query(query):
    """Execute SQL query and return results."""
    if not sql_available:
        return "Error: SQL database is not available"
    
    global latest_query_result, latest_query_columns
    
    if not query.strip().upper().startswith('SELECT'):
        return "Error: Only SELECT queries are allowed for SQL"
    
    # Check for dangerous operations
    dangerous_patterns = [r'\bDROP\b', r'\bDELETE\b', r'\bTRUNCATE\b', r'\bUPDATE\b', r'\bINSERT\b', r'\bALTER\b', r'\bCREATE\b']
    for pattern in dangerous_patterns:
        if re.search(pattern, query, re.IGNORECASE):
            return f"Error: Dangerous SQL operation detected ({pattern.strip('\\b')}). Only SELECT allowed."
    
    try:
        with sql_engine.connect() as conn:
            result = conn.execute(text(query))
            rows = result.fetchall()
            
            if not rows:
                latest_query_result = None
                latest_query_columns = None
                return "(no SQL rows returned)"
            
            columns = result.keys() if hasattr(result, 'keys') else [str(i) for i in range(len(rows[0]))]
            latest_query_result = rows
            latest_query_columns = columns
            
            # Format as markdown table
            header = "| " + " | ".join(str(col) for col in columns) + " |"
            separator = "| " + " | ".join(["---"] * len(columns)) + " |"
            
            out = [header, separator]
            for row in rows:
                out.append("| " + " | ".join(str(val).replace("|", "\\|") for val in row) + " |")
            
            return "\n".join(out)
    except Exception as e:
        latest_query_result = None
        latest_query_columns = None
        return f"Error executing SQL query: {str(e)}"

# --- Visualization Tools ---
def create_pie_chart(data_input):
    """Create pie chart from latest query results."""
    global latest_query_result, latest_query_columns
    
    if not latest_query_result or not latest_query_columns:
        return "Error: No query results available. Please run a query first."
    
    try:
        params = json.loads(data_input)
        labels_column = params.get("labels_column")
        values_column = params.get("values_column")
        title = params.get("title", "Pie Chart")
        
        if not labels_column or not values_column:
            return f"Error: 'labels_column' and 'values_column' required. Available: {', '.join(latest_query_columns)}"
        
        # Convert to DataFrame for easier processing
        if isinstance(latest_query_result[0], dict):  # MongoDB results
            df = pd.DataFrame(latest_query_result)
        else:  # SQL results
            df = pd.DataFrame(latest_query_result, columns=latest_query_columns)
        
        if labels_column not in df.columns:
            return f"Error: Column '{labels_column}' not found. Available: {', '.join(df.columns)}"
        
        if values_column == "count":
            counts = df[labels_column].value_counts()
            labels = counts.index
            values = counts.values
        elif values_column not in df.columns:
            return f"Error: Column '{values_column}' not found. Available: {', '.join(df.columns)}"
        else:
            aggregated = df.groupby(labels_column)[values_column].sum()
            labels = aggregated.index
            values = aggregated.values
        
        plt.figure(figsize=(10, 8))
        plt.pie(values, labels=labels, autopct='%1.1f%%', startangle=140)
        plt.title(title)
        plt.axis('equal')
        
        chart_path = "pie_chart.png"
        plt.savefig(chart_path, bbox_inches='tight', dpi=300)
        plt.close()
        
        return f"Pie chart saved as '{chart_path}'"
    except Exception as e:
        return f"Error creating pie chart: {str(e)}"

def create_bar_chart(data_input):
    """Create bar chart from latest query results."""
    global latest_query_result, latest_query_columns
    
    if not latest_query_result or not latest_query_columns:
        return "Error: No query results available. Please run a query first."
    
    try:
        params = json.loads(data_input)
        x_column = params.get("x_column")
        y_column = params.get("y_column")
        title = params.get("title", "Bar Chart")
        
        if not x_column or not y_column:
            return f"Error: 'x_column' and 'y_column' required. Available: {', '.join(latest_query_columns)}"
        
        # Convert to DataFrame
        if isinstance(latest_query_result[0], dict):  # MongoDB results
            df = pd.DataFrame(latest_query_result)
        else:  # SQL results
            df = pd.DataFrame(latest_query_result, columns=latest_query_columns)
        
        if x_column not in df.columns:
            return f"Error: Column '{x_column}' not found. Available: {', '.join(df.columns)}"
        
        if y_column == "count":
            counts = df[x_column].value_counts()
            x_values = counts.index.astype(str)
            y_values = counts.values
            y_label = "Count"
        elif y_column not in df.columns:
            return f"Error: Column '{y_column}' not found. Available: {', '.join(df.columns)}"
        else:
            df[y_column] = pd.to_numeric(df[y_column], errors='coerce')
            if df[y_column].isnull().all():
                return f"Error: Column '{y_column}' contains no numeric data"
            
            if df[x_column].duplicated().any():
                aggregated = df.groupby(x_column)[y_column].sum().reset_index()
            else:
                aggregated = df[[x_column, y_column]].copy()
            
            x_values = aggregated[x_column].astype(str)
            y_values = aggregated[y_column].fillna(0)
            y_label = y_column
        
        plt.figure(figsize=(12, 7))
        plt.bar(x_values, y_values)
        plt.xlabel(x_column)
        plt.ylabel(y_label)
        plt.title(title)
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        
        chart_path = "bar_chart.png"
        plt.savefig(chart_path, bbox_inches='tight', dpi=300)
        plt.close()
        
        return f"Bar chart saved as '{chart_path}'"
    except Exception as e:
        return f"Error creating bar chart: {str(e)}"

def get_database_info(tool_input=None):
    """Get information about available databases and their capabilities."""
    info = []
    
    if mongodb_available:
        try:
            collections = mongodb_db.list_collection_names()
            info.append(f"✅ MongoDB: Connected to {mongodb_db.name} with {len(collections)} collections")
            info.append(f"   Collections: {', '.join(collections[:5])}{'...' if len(collections) > 5 else ''}")
        except Exception as e:
            info.append(f"❌ MongoDB: Error getting info - {str(e)}")
    else:
        info.append("❌ MongoDB: Not available")
    
    if sql_available:
        try:
            tables = sql_inspector.get_table_names()
            db_type = sql_engine.name
            info.append(f"✅ SQL ({db_type.upper()}): Connected with {len(tables)} tables")
            info.append(f"   Tables: {', '.join(tables[:5])}{'...' if len(tables) > 5 else ''}")
        except Exception as e:
            info.append(f"❌ SQL: Error getting info - {str(e)}")
    else:
        info.append("❌ SQL: Not available")
    
    return "\n".join(info)

# --- Tools List ---
tools = [
    get_database_info,
    # MongoDB tools
    mongo_db_list_collections,
    mongo_db_schema,
    mongo_db_query,
    mongo_count_documents,
    # SQL tools
    sql_db_list_tables,
    sql_db_schema,
    sql_db_query,
    # Visualization tools
    create_pie_chart,
    create_bar_chart
]

# --- Agent Instructions ---
instructions = [
    "You are a unified database agent that can work with both MongoDB and SQL databases.",
    "You have access to both MongoDB and SQL databases. Use get_database_info to see what's available.",
    "",
    "WORKFLOW:",
    "1. Start by checking available databases with get_database_info",
    "2. Based on the user's question, determine which database type is most appropriate",
    "3. List collections/tables in the chosen database",
    "4. Examine schemas before writing queries",
    "5. Execute queries and format results clearly",
    "6. Create visualizations if requested",
    "",
    """When building a MongoDB query:

1. Start with identifying the target collection
2. Construct the filter criteria using appropriate operators:
   - Equality: { field: value }
   - Comparison: { field: { $gt: value } }
   - Array: { field: { $in: [value1, value2] } }
   - Logical: { $and: [ {condition1}, {condition2} ] }
   - Text search: { $text: { $search: "phrase" } }

3. Consider projection to limit returned fields: { field1: 1, field2: 1 }
4. Add sorting when helpful: { field: 1 } for ascending, { field: -1 } for descending
5. Always include skip and limit for pagination"""

    "",
    "SQL GUIDELINES:",
    "- Use sql_db_list_tables to see available tables",
    "- Use sql_db_schema to understand table structure",
    "- Use sql_db_query with SELECT statements only",
    "- Always LIMIT results to 5 unless specified otherwise",
    "- Never use dangerous operations (DROP, DELETE, UPDATE, etc.)",
    "",
    "VISUALIZATION:",
    "- Use create_pie_chart or create_bar_chart after running queries",
    "- Provide JSON config: {\"title\": \"Chart Title\", \"labels_column\": \"col1\", \"values_column\": \"col2\"}",
    "- For counting: use \"values_column\": \"count\"",
    "",
    "ERROR HANDLING:",
    "- If one database is unavailable, gracefully use the other",
    "- Provide clear error messages with available alternatives",
    "- Suggest corrections for invalid queries",
    "",
    "Return format for SQL: ",
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
        +----------------+-------------+"""

       """Return format for MongoDB:
       
       """ 
]

# --- Initialize and Run Agent ---
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
        print("🚀 Unified MongoDB + SQL Agent with Visualization started!")
        print("📊 Available features: Database queries, schema inspection, data visualization")
        print("🔍 Type 'exit' or 'quit' to end.\n")
        
        while True:
            try:
                question = input("💬 Enter your question: ")
                if question.lower() in ['exit', 'quit']:
                    print("👋 Goodbye!")
                    break
                
                print("\n" + "="*50)
                agent.print_response(question, stream=True)
                print("="*50 + "\n")
                
            except KeyboardInterrupt:
                print("\n👋 Goodbye!")
                break
            except Exception as e:
                print(f"❌ Error: {str(e)}")
                traceback.print_exc()

except Exception as e:
    print(f"❌ Error initializing agent: {str(e)}")
    traceback.print_exc()