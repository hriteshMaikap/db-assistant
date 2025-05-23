import os
import re
import sys
import json
import datetime
import traceback
import pandas as pd
from dotenv import load_dotenv
from typing import Dict, List, Any, Optional

# MongoDB imports
from pymongo import MongoClient
from bson import ObjectId, json_util

# SQL imports
from sqlalchemy import create_engine, text, inspect

# Visualization imports
import seaborn as sns
import matplotlib.pyplot as plt

# AGNO imports
from agno.agent import Agent
from agno.models.groq import Groq

# Load environment variables
load_dotenv()
groq_api_key = os.getenv("GROQ_API_KEY")
if not groq_api_key:
    raise ValueError("GROQ_API_KEY not set in .env")

# Global variables for both database types
latest_query_result = None
latest_query_columns = None
current_db_type = None  # 'mongo' or 'sql'

# Database connection information
db_connections = {
    'mongo': {
        'available': False,
        'client': None,
        'db': None,
        'schema_cache': {}  # Cache for MongoDB schemas
    },
    'sql': {
        'available': False,
        'engine': None,
        'inspector': None,
        'schema_cache': {}  # Cache for SQL schemas
    }
}

# --------------------------------
# MongoDB Connection Setup
# --------------------------------
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DB_NAME = os.getenv("MONGODB_DB_NAME", "university_management")
MONGODB_SCHEMA_SAMPLE_SIZE = int(os.getenv("MONGODB_SCHEMA_SAMPLE_SIZE", "100"))

print(f"Attempting to connect to MongoDB: {MONGODB_URI}/{MONGODB_DB_NAME}")

try:
    mongo_client = MongoClient(MONGODB_URI)
    mongo_db = mongo_client[MONGODB_DB_NAME]
    # Test connection by getting server info
    mongo_client.server_info()
    print("MongoDB connection initialized successfully!")
    db_connections['mongo']['available'] = True
    db_connections['mongo']['client'] = mongo_client
    db_connections['mongo']['db'] = mongo_db
except Exception as e:
    print(f"MongoDB connection error: {str(e)}")
    traceback.print_exc()

# --------------------------------
# SQL Connection Setup
# --------------------------------
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASS = os.getenv("MYSQL_PASS")
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT")
MYSQL_DB   = os.getenv("MYSQL_DB")

print(f"Attempting to connect to MySQL: {MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB} as {MYSQL_USER}")

try:
    sql_engine = create_engine(
        f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
    )
    
    # Test the connection
    with sql_engine.connect() as conn:
        conn.execute(text("SELECT 1"))
        print("MySQL connection successful!")
    db_connections['sql']['available'] = True
    db_connections['sql']['engine'] = sql_engine
except Exception as e:
    print(f"MySQL connection error: {str(e)}")
    print("Attempting to use SQLite instead...")
    
    # Fall back to SQLite if MySQL connection fails
    try:
        sqlite_path = os.path.join(os.path.dirname(__file__), "Chinook.db")
        print(f"Looking for SQLite database at: {sqlite_path}")
        
        if os.path.exists(sqlite_path):
            sql_engine = create_engine(f"sqlite:///{sqlite_path}")
            print("SQLite connection successful!")
            db_connections['sql']['available'] = True
            db_connections['sql']['engine'] = sql_engine
        else:
            print(f"ERROR: SQLite database file not found at {sqlite_path}")
    except Exception as e:
        print(f"SQLite connection error: {str(e)}")

# Initialize SQL inspector if SQL connection is available
if db_connections['sql']['available']:
    try:
        inspector = inspect(db_connections['sql']['engine'])
        print(f"SQL database metadata inspection initialized")
        db_connections['sql']['inspector'] = inspector
    except Exception as e:
        print(f"Error initializing SQL inspector: {str(e)}")
        traceback.print_exc()
        db_connections['sql']['available'] = False

# Check if at least one database type is available
if not db_connections['mongo']['available'] and not db_connections['sql']['available']:
    print("ERROR: No database connections available. Please check your configuration.")
    sys.exit(1)

# Set default database type based on availability
if db_connections['mongo']['available']:
    current_db_type = 'mongo'
elif db_connections['sql']['available']:
    current_db_type = 'sql'

# --------------------------------
# Helper Classes
# --------------------------------

# Custom JSON encoder to handle MongoDB ObjectId and other BSON types
class MongoJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)

# --------------------------------
# Common Functions
# --------------------------------

def set_current_db_type(db_type):
    """Set the current database type to use (mongo/sql)"""
    global current_db_type
    
    if db_type.lower() not in ['mongo', 'sql']:
        return f"Error: Invalid database type '{db_type}'. Valid options are 'mongo' or 'sql'."
    
    requested_type = db_type.lower()
    
    if requested_type == 'mongo' and not db_connections['mongo']['available']:
        return "Error: MongoDB connection is not available."
    
    if requested_type == 'sql' and not db_connections['sql']['available']:
        return "Error: SQL database connection is not available."
    
    current_db_type = requested_type
    return f"Database type set to: {current_db_type}"

# --------------------------------
# Schema Cache Management (New Functions)
# --------------------------------

def cache_sql_table_schema(table_name):
    """Cache schema for a specific SQL table."""
    if not db_connections['sql']['available']:
        return {"error": "SQL database not available"}
    
    try:
        inspector = db_connections['sql']['inspector']
        engine = db_connections['sql']['engine']
        
        # Get table schema
        columns = inspector.get_columns(table_name)
        col_lines = [
            f"{col['name']} {col['type']}{' PRIMARY KEY' if col.get('primary_key') else ''}"
            for col in columns
        ]
        create_sql = f"CREATE TABLE {table_name} (\n    " + ",\n    ".join(col_lines) + "\n)"
        
        # Get sample data (only 2 rows to reduce size)
        with engine.connect() as conn:
            sample = conn.execute(text(f"SELECT * FROM {table_name} LIMIT 2")).fetchall()
            sample_str = ""
            if sample:
                colnames = [c["name"] for c in columns]
                sample_str += "SAMPLE DATA:\n"
                sample_str += "\t".join(colnames) + "\n"
                for row in sample:
                    sample_str += "\t".join(str(val) for val in row) + "\n"
            else:
                sample_str += "(no data in table)\n"
        
        # Cache the schema
        db_connections['sql']['schema_cache'][table_name] = {
            "columns": columns,
            "create_sql": create_sql,
            "sample": sample_str,
            "timestamp": datetime.datetime.now()
        }
        
        return db_connections['sql']['schema_cache'][table_name]
    except Exception as e:
        return {"error": f"Error caching schema for table {table_name}: {str(e)}"}

def cache_mongo_collection_schema(collection_name):
    """Cache schema for a specific MongoDB collection."""
    if not db_connections['mongo']['available']:
        return {"error": "MongoDB not available"}
    
    try:
        mongo_db = db_connections['mongo']['db']
        collection = mongo_db[collection_name]
        
        # Get sample documents (only 2 to reduce size)
        cursor = collection.find().limit(2)
        documents = list(cursor)
        
        if not documents:
            db_connections['mongo']['schema_cache'][collection_name] = {
                "empty": True,
                "timestamp": datetime.datetime.now()
            }
            return db_connections['mongo']['schema_cache'][collection_name]
        
        # Infer schema from sampled documents
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
        
        # Cache the schema
        db_connections['mongo']['schema_cache'][collection_name] = {
            "schema": schema,
            "sample": documents[0] if documents else None,
            "timestamp": datetime.datetime.now()
        }
        
        return db_connections['mongo']['schema_cache'][collection_name]
    except Exception as e:
        return {"error": f"Error caching schema for collection {collection_name}: {str(e)}"}

# --------------------------------
# MongoDB Tools for AGNO Agent
# --------------------------------

def mongo_db_list_collections(tool_input=None):
    """List all collections in MongoDB database"""
    if current_db_type != 'mongo':
        return "Error: Currently connected to SQL database. Use 'set_current_db_type' to switch to MongoDB."
    
    try:
        mongo_db = db_connections['mongo']['db']
        collections = mongo_db.list_collection_names()
        return ", ".join(collections)
    except Exception as e:
        traceback.print_exc()
        return f"Error listing MongoDB collections: {str(e)}"

def mongo_db_schema(tool_input: str):
    """Get schema for specified MongoDB collections"""
    if current_db_type != 'mongo':
        return "Error: Currently connected to SQL database. Use 'set_current_db_type' to switch to MongoDB."
    
    try:
        mongo_db = db_connections['mongo']['db']
        output = []
        collection_names = [name.strip() for name in tool_input.split(",") if name.strip()]
        
        for collection in collection_names:
            # Check cache first
            if collection in db_connections['mongo']['schema_cache']:
                cache_entry = db_connections['mongo']['schema_cache'][collection]
                # Check if cache is recent (less than 1 hour old)
                if (datetime.datetime.now() - cache_entry['timestamp']).seconds < 3600:
                    if 'empty' in cache_entry and cache_entry['empty']:
                        output.append(f"## Collection: {collection}\n\nNo documents found.")
                        continue
                    
                    # Format schema from cache
                    schema_md = f"## Collection: {collection}\n\n"
                    schema_md += "### Schema:\n\n"
                    schema_md += "| Field | Type | Example |\n"
                    schema_md += "|-------|------|--------|\n"
                    
                    for field, info in cache_entry['schema'].items():
                        field_type = info["type"]
                        if isinstance(field_type, list):
                            field_type = " or ".join(field_type)
                        
                        example = info["example"]
                        if isinstance(example, (dict, list)):
                            example_str = "complex structure"
                        else:
                            example_str = str(example)
                            if len(example_str) > 30:
                                example_str = example_str[:27] + "..."
                        
                        schema_md += f"| {field} | {field_type} | {example_str} |\n"
                    
                    if cache_entry['sample']:
                        schema_md += "\n### Sample Document:\n\n"
                        schema_md += f"```json\n{json.dumps(cache_entry['sample'], cls=MongoJSONEncoder)}\n```\n"
                    
                    output.append(schema_md)
                    continue
            
            # Cache miss or outdated cache, get fresh schema
            cache_result = cache_mongo_collection_schema(collection)
            
            if 'error' in cache_result:
                output.append(f"Error with collection {collection}: {cache_result['error']}")
                continue
                
            if 'empty' in cache_result and cache_result['empty']:
                output.append(f"## Collection: {collection}\n\nNo documents found.")
                continue
            
            # Format schema
            schema_md = f"## Collection: {collection}\n\n"
            schema_md += "### Schema:\n\n"
            schema_md += "| Field | Type | Example |\n"
            schema_md += "|-------|------|--------|\n"
            
            for field, info in cache_result['schema'].items():
                field_type = info["type"]
                if isinstance(field_type, list):
                    field_type = " or ".join(field_type)
                
                example = info["example"]
                if isinstance(example, (dict, list)):
                    example_str = "complex structure"
                else:
                    example_str = str(example)
                    if len(example_str) > 30:
                        example_str = example_str[:27] + "..."
                
                schema_md += f"| {field} | {field_type} | {example_str} |\n"
            
            if cache_result['sample']:
                schema_md += "\n### Sample Document:\n\n"
                schema_md += f"```json\n{json.dumps(cache_result['sample'], cls=MongoJSONEncoder)}\n```\n"
            
            output.append(schema_md)
        
        return "\n\n".join(output)
    except Exception as e:
        traceback.print_exc()
        return f"General error with MongoDB schema extraction: {str(e)}"

def mongo_query_checker(query_str):
    """Check MongoDB query for correctness"""
    if current_db_type != 'mongo':
        return "Error: Currently connected to SQL database. Use 'set_current_db_type' to switch to MongoDB."
    
    try:
        # Parse the query string to verify it's valid JSON
        query = json.loads(query_str)
        
        # Check for common issues
        for key, value in query.items():
            # Check for comparison operators without $ prefix
            if key in ["gt", "gte", "lt", "lte", "eq", "ne", "in", "nin"] and not key.startswith("$"):
                return f"Warning: Operator '{key}' should be '${key}'. Correct JSON: {json.dumps(query_str, indent=2)}"
        
        # Return the validated query
        return f"```json\n{json.dumps(query, indent=2)}\n```"
    except json.JSONDecodeError:
        return f"Error: Invalid JSON in query. Please check the syntax: {query_str}"
    except Exception as e:
        return f"Error checking MongoDB query: {str(e)}"

def mongo_db_query(query_input):
    """Execute MongoDB query and return results"""
    if current_db_type != 'mongo':
        return "Error: Currently connected to SQL database. Use 'set_current_db_type' to switch to MongoDB."
    
    global latest_query_result, latest_query_columns
    
    try:
        mongo_db = db_connections['mongo']['db']
        query_params = json.loads(query_input)
        collection_name = query_params.get("collection")
        
        if not collection_name:
            return "Error: Collection name is required"

        pipeline = query_params.get("pipeline")
        collection = mongo_db[collection_name]

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
            return "(no documents returned)"
        
        # Format the results as a table
        all_fields = set()
        for doc in documents:
            all_fields.update(doc.keys())
        
        # For find queries, _id is often removed unless projected.
        if not pipeline and '_id' in all_fields and (not query_params.get("projection") or '_id' not in query_params.get("projection", {})):
            all_fields.remove('_id')
        
        columns = sorted(list(all_fields))
        
        latest_query_result = documents
        latest_query_columns = columns
        
        # Format output as markdown table for better readability
        header = "| " + " | ".join(columns) + " |"
        separator = "| " + " | ".join(["---"] * len(columns)) + " |"
        
        rows = []
        for doc in documents:
            row_values = []
            for col in columns:
                val = doc.get(col, "")
                if isinstance(val, (dict, list)):
                    val = json.dumps(val, cls=MongoJSONEncoder)
                elif isinstance(val, ObjectId):
                    val = str(val)
                    
                # Truncate long values and escape pipe characters
                str_val = str(val)
                if len(str_val) > 50:
                    str_val = str_val[:47] + "..."
                str_val = str_val.replace("|", "\\|")
                    
                row_values.append(str_val)
            rows.append("| " + " | ".join(row_values) + " |")
        
        output = [header, separator] + rows
        return "\n".join(output)
    
    except json.JSONDecodeError:
        latest_query_result = None
        latest_query_columns = None
        return f"Error: Invalid JSON in query input: {query_input}"
    except Exception as e:
        latest_query_result = None
        latest_query_columns = None
        traceback.print_exc()
        return f"Error executing MongoDB query: {str(e)}"

def mongo_count_documents(tool_input):
    """Count documents in MongoDB collection"""
    if current_db_type != 'mongo':
        return "Error: Currently connected to SQL database. Use 'set_current_db_type' to switch to MongoDB."
    
    try:
        mongo_db = db_connections['mongo']['db']
        params = json.loads(tool_input)
        collection_name = params.get("collection")
        filter_query = params.get("filter", {})
        
        if not collection_name:
            return "Error: Collection name is required"
        
        collection = mongo_db[collection_name]
        count = collection.count_documents(filter_query)
        return str(count)
    except json.JSONDecodeError:
        return f"Error: Invalid JSON in count input: {tool_input}"
    except Exception as e:
        traceback.print_exc()
        return f"Error counting MongoDB documents: {str(e)}"

# --------------------------------
# SQL Tools for AGNO Agent
# --------------------------------

def sql_db_list_tables(tool_input=None):
    """List all tables in SQL database"""
    if current_db_type != 'sql':
        return "Error: Currently connected to MongoDB. Use 'set_current_db_type' to switch to SQL."
    
    try:
        inspector = db_connections['sql']['inspector']
        tables = inspector.get_table_names()
        if not tables:
            return "No tables found in database."
        return ", ".join(tables)
    except Exception as e:
        error_msg = f"Error listing SQL tables: {str(e)}"
        print(error_msg)
        traceback.print_exc()
        return error_msg

def sql_db_schema(table_names):
    """Get schema for specified SQL tables"""
    if current_db_type != 'sql':
        return "Error: Currently connected to MongoDB. Use 'set_current_db_type' to switch to SQL."
    
    try:
        inspector = db_connections['sql']['inspector']
        output = []
        table_names = [name.strip() for name in table_names.split(",") if name.strip()]
        all_tables = inspector.get_table_names()
        
        # Check if tables exist
        invalid_tables = [t for t in table_names if t not in all_tables]
        if invalid_tables:
            return f"Error: The following tables do not exist: {', '.join(invalid_tables)}. Available tables are: {', '.join(all_tables)}"
        
        for table in table_names:
            # Check cache first
            if table in db_connections['sql']['schema_cache']:
                cache_entry = db_connections['sql']['schema_cache'][table]
                # Check if cache is recent (less than 1 hour old)
                if (datetime.datetime.now() - cache_entry['timestamp']).seconds < 3600:
                    output.append(f"TABLE: {table}\n{cache_entry['create_sql']}\n{cache_entry['sample']}")
                    continue
            
            # Cache miss or outdated cache, get fresh schema
            cache_result = cache_sql_table_schema(table)
            
            if 'error' in cache_result:
                output.append(f"Error with table {table}: {cache_result['error']}")
                continue
                
            output.append(f"TABLE: {table}\n{cache_result['create_sql']}\n{cache_result['sample']}")
        
        return "\n\n".join(output)
    except Exception as e:
        error_msg = f"General error with SQL schema extraction: {str(e)}"
        print(error_msg)
        traceback.print_exc()
        return error_msg

def sql_db_query_checker(query):
    """Check SQL query for correctness"""
    if current_db_type != 'sql':
        return "Error: Currently connected to MongoDB. Use 'set_current_db_type' to switch to SQL."
    
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
    db_type = db_connections['sql']['engine'].name
    
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
    inspector = db_connections['sql']['inspector']
    tables = inspector.get_table_names()
    
    # Extract table names from FROM and JOIN clauses (basic extraction, not perfect)
    query_tables = re.findall(r'(?:FROM|JOIN)\s+([a-zA-Z0-9_]+)', query, re.IGNORECASE)
    missing_tables = [table for table in query_tables if table not in tables]
    
    if missing_tables:
        return f"Error: The following tables do not exist: {', '.join(missing_tables)}. Available tables are: {', '.join(tables)}"
    
    return f"```sql\n{query}\n```"

def sql_db_query(query):
    """Execute SQL query and return results"""
    if current_db_type != 'sql':
        return "Error: Currently connected to MongoDB. Use 'set_current_db_type' to switch to SQL."
    
    global latest_query_result, latest_query_columns
    
    # Validate it's a SELECT query
    if not query.strip().upper().startswith('SELECT'):
        return "Error: Only SELECT queries are allowed. Please provide a SELECT statement."
    
    try:
        engine = db_connections['sql']['engine']
        with engine.connect() as conn:
            print(f"Executing SQL query: {query}")
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
                # Truncate long values and escape pipe characters
                formatted_row = []
                for val in row:
                    str_val = str(val)
                    if len(str_val) > 50:
                        str_val = str_val[:47] + "..."
                    str_val = str_val.replace("|", "\\|")
                    formatted_row.append(str_val)
                
                out.append("| " + " | ".join(formatted_row) + " |")
            
            return "\n".join(out)
    except Exception as e:
        latest_query_result = None
        latest_query_columns = None
        error_msg = f"Error executing SQL query: {str(e)}"
        print(error_msg)
        traceback.print_exc()
        
        # Extract table names from the query
        tables = re.findall(r'(?:FROM|JOIN)\s+([a-zA-Z0-9_]+)', query, re.IGNORECASE)
        available_tables = db_connections['sql']['inspector'].get_table_names()
        
        # Provide helpful error context
        context = f"\n\nAvailable tables: {', '.join(available_tables)}"
        if tables:
            # Check if the tables mentioned exist
            missing_tables = [t for t in tables if t not in available_tables]
            if missing_tables:
                context += f"\n\nError details: Tables {', '.join(missing_tables)} do not exist."
        
        return error_msg + context

def get_sql_db_capabilities():
    """Get SQL database capabilities info"""
    if current_db_type != 'sql':
        return "Error: Currently connected to MongoDB. Use 'set_current_db_type' to switch to SQL."
    
    db_type = db_connections['sql']['engine'].name
    
    if db_type == 'sqlite':
        return """Database: SQLite
Key features: strftime() for dates, basic aggregations (COUNT, SUM, AVG, MIN, MAX), GROUP_CONCAT
Example: SELECT strftime('%Y', date_column) as year, COUNT(*) FROM table GROUP BY year"""
    elif db_type == 'mysql':
        return """Database: MySQL
Key features: Date functions (YEAR(), MONTH()), string functions (CONCAT, SUBSTRING), 
JSON functions, window functions
Example: SELECT YEAR(date_column) as year, COUNT(*) FROM table GROUP BY year"""
    else:
        return f"""Database: {db_type}
Generic SQL capabilities: SELECT, FROM, WHERE, GROUP BY, aggregation functions"""

# --------------------------------
# Visualization Tools (Optimized)
# --------------------------------

def visualize_pie_chart(tool_input: str):
    """Create pie chart from query results"""
    global latest_query_result, latest_query_columns
    
    if latest_query_result is None:
        return "Error: No query results available. Run a query first."

    try:
        params = json.loads(tool_input)
        labels_column = params.get('labels_column')
        values_column = params.get('values_column')
        
        if not labels_column or not values_column:
            return f"Error: 'labels_column' and 'values_column' required. Available: {latest_query_columns}"
        
        # Process data based on current DB type
        if current_db_type == 'mongo':
            df = pd.DataFrame(latest_query_result)
            if values_column == 'count':
                counts = df[labels_column].value_counts()
                labels = counts.index
                values = counts.values
            else:
                grouped = df.groupby(labels_column)[values_column].sum()
                labels = grouped.index
                values = grouped.values
        else:  # SQL
            try:
                idx_labels = list(latest_query_columns).index(labels_column)
                if values_column == 'count':
                    label_counts = {}
                    for row in latest_query_result:
                        label = row[idx_labels]
                        label_counts[label] = label_counts.get(label, 0) + 1
                    labels = list(label_counts.keys())
                    values = list(label_counts.values())
                else:
                    idx_values = list(latest_query_columns).index(values_column)
                    aggregated = {}
                    for row in latest_query_result:
                        label = row[idx_labels]
                        value = float(row[idx_values])
                        aggregated[label] = aggregated.get(label, 0) + value
                    labels = list(aggregated.keys())
                    values = list(aggregated.values())
            except Exception as e:
                return f"Error processing data: {str(e)}"
        
        # Create pie chart
        plt.figure(figsize=(10, 8))
        plt.pie(values, labels=labels, autopct='%1.1f%%')
        plt.title(f"Pie Chart: {values_column} by {labels_column}")
        plt.axis('equal')
        chart_path = "pie_chart.png"
        plt.savefig(chart_path)
        plt.close()
        
        return f"Pie chart saved as '{chart_path}'."
    except Exception as e:
        return f"Error creating pie chart: {str(e)}"

def visualize_bar_chart(tool_input: str):
    """Create bar chart from query results"""
    global latest_query_result, latest_query_columns
    
    try:
        params = json.loads(tool_input)
        
        # If direct data provided
        if 'labels' in params and 'values' in params:
            plt.figure(figsize=(10, 6))
            plt.bar(params['labels'], params['values'])
            plt.title(params.get('title', 'Bar Chart'))
            plt.xticks(rotation=45)
            plt.tight_layout()
            chart_path = "bar_chart.png"
            plt.savefig(chart_path)
            plt.close()
            return f"Bar chart saved as '{chart_path}'."
        
        # Otherwise use query results
        if not latest_query_result:
            return "Error: No query results available. Run a query first."
            
        x_col = params.get('x_column')
        y_col = params.get('y_column')
        
        if not x_col or not y_col:
            return f"Error: 'x_column' and 'y_column' required. Available: {latest_query_columns}"
        
        # Process data based on DB type
        if current_db_type == 'mongo':
            df = pd.DataFrame(latest_query_result)
            if y_col == 'count':
                counts = df[x_col].value_counts().reset_index()
                x_vals = counts['index']
                y_vals = counts[x_col]
            else:
                grouped = df.groupby(x_col)[y_col].sum().reset_index()
                x_vals = grouped[x_col]
                y_vals = grouped[y_col]
        else:  # SQL
            try:
                idx_x = list(latest_query_columns).index(x_col)
                if y_col == 'count':
                    # Count occurrences
                    count_dict = {}
                    for row in latest_query_result:
                        val = row[idx_x]
                        count_dict[val] = count_dict.get(val, 0) + 1
                    x_vals = list(count_dict.keys())
                    y_vals = list(count_dict.values())
                else:
                    idx_y = list(latest_query_columns).index(y_col)
                    # Aggregate values
                    agg_dict = {}
                    for row in latest_query_result:
                        x_val = row[idx_x]
                        y_val = float(row[idx_y])
                        agg_dict[x_val] = agg_dict.get(x_val, 0) + y_val
                    x_vals = list(agg_dict.keys())
                    y_vals = list(agg_dict.values())
            except Exception as e:
                return f"Error processing data: {str(e)}"
        
        # Create bar chart
        plt.figure(figsize=(12, 7))
        plt.bar(x_vals, y_vals)
        plt.xlabel(x_col)
        plt.ylabel(y_col if y_col != 'count' else 'Count')
        plt.title(params.get('title', f"Bar Chart: {y_col} by {x_col}"))
        plt.xticks(rotation=45)
        plt.tight_layout()
        chart_path = "bar_chart.png"
        plt.savefig(chart_path)
        plt.close()
        
        return f"Bar chart saved as '{chart_path}'."
    except Exception as e:
        return f"Error creating bar chart: {str(e)}"

# --------------------------------
# Get Database Context (Reduced version)
# --------------------------------

def get_current_db_context():
    """Get a minimal context about the current database."""
    
    if current_db_type == 'mongo':
        try:
            mongo_db = db_connections['mongo']['db']
            collections = mongo_db.list_collection_names()
            return f"MongoDB database with collections: {', '.join(collections)}"
        except Exception as e:
            return f"MongoDB database (error retrieving collections: {str(e)})"
    else:  # SQL
        try:
            inspector = db_connections['sql']['inspector']
            tables = inspector.get_table_names()
            engine_name = db_connections['sql']['engine'].name
            return f"{engine_name.capitalize()} database with tables: {', '.join(tables)}"
        except Exception as e:
            return f"SQL database (error retrieving tables: {str(e)})"

# --------------------------------
# Main Agent Setup
# --------------------------------

# Efficient, concise tools list
tools = [
    # Database selection
    set_current_db_type,
    
    # MongoDB tools
    mongo_db_list_collections,
    mongo_db_schema,
    mongo_query_checker,
    mongo_db_query,
    mongo_count_documents,
    
    # SQL tools
    sql_db_list_tables,
    sql_db_schema,
    sql_db_query_checker,
    sql_db_query,
    get_sql_db_capabilities,
    
    # Visualization tools
    visualize_pie_chart,
    visualize_bar_chart
]

# Condensed, efficient instructions
instructions = [
    f"You are a database assistant that works with both MongoDB and SQL databases. Current database: {current_db_type}.",
    f"MongoDB available: {db_connections['mongo']['available']}. SQL available: {db_connections['sql']['available']}.",
    "Use 'set_current_db_type' with 'mongo' or 'sql' to switch databases.",
    "",
    "STEPS FOR MONGODB QUERIES:",
    "1. List collections → Check schema → Write query → Execute",
    "2. Format: {\"collection\": \"name\", \"filter\": {}, \"limit\": 5}",
    "",
    "STEPS FOR SQL QUERIES:",
    "1. List tables → Check schema → Write query → Execute",
    "2. Only use SELECT statements with valid tables/columns",
    "", 
    "VISUALIZATION:",
    "- Run a query first, then visualize with pie/bar charts",
    "- For both: specify column names from result",
    "",
    "RULES:",
    "- Always limit to 5 results unless asked otherwise",
    "- Use the correct tools for the current database type",
    "- Never try MongoDB operations on SQL or vice versa",
    "- Never use destructive operations (DROP, UPDATE, etc.)",
    "",
    f"CURRENT CONTEXT: {get_current_db_context()}"
]

# Create token-efficient agent
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
            print("Optimized Database Assistant started. Type 'exit' or 'quit' to end.")
            print(f"Current database type: {current_db_type}")
            print(f"MongoDB available: {db_connections['mongo']['available']}")
            print(f"SQL available: {db_connections['sql']['available']}")
            
            while True:
                question = input("Enter your question: ")
                if question.lower() in ['exit', 'quit']:
                    print("Exiting...")
                    break
                
                try:
                    # Update instructions with current context
                    updated_instructions = instructions.copy()
                    updated_instructions[0] = f"You are a database assistant that works with both MongoDB and SQL databases. Current database: {current_db_type}."
                    updated_instructions[-1] = f"CURRENT CONTEXT: {get_current_db_context()}"

                    # Re-initialize the Agent with minimal context
                    agent = Agent(
                        model=Groq(id="llama-3.3-70b-versatile", api_key=groq_api_key),
                        tools=tools,
                        instructions=updated_instructions,
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