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
from typing import Dict, List, Any, Optional
from agno.agent import Agent
from agno.models.groq import Groq
import matplotlib.pyplot as plt # Added import

# Load environment variables
load_dotenv()
groq_api_key = os.getenv("GROQ_API_KEY")
if not groq_api_key:
    raise ValueError("GROQ_API_KEY not set in .env")

# MongoDB connection parameters
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DB_NAME = os.getenv("MONGODB_DB_NAME", "university_management")
MONGODB_SCHEMA_SAMPLE_SIZE = int(os.getenv("MONGODB_SCHEMA_SAMPLE_SIZE", "100"))

print(f"Connecting to MongoDB: {MONGODB_URI}/{MONGODB_DB_NAME}")

# Create MongoDB client (using synchronous client)
try:
    client = MongoClient(MONGODB_URI)
    db = client[MONGODB_DB_NAME]
    print("MongoDB connection initialized successfully!")
except Exception as e:
    print(f"MongoDB connection error: {str(e)}")
    traceback.print_exc()
    sys.exit(1)

# Global variable to store the latest query result
latest_query_result = None
latest_query_columns = None

# Custom JSON encoder to handle MongoDB ObjectId and other BSON types
class MongoJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)

# --- MongoDB Tools for AGNO Agent ---

def mongo_db_list_collections(tool_input=None):
    """List all collections in the MongoDB database.
    
    Args:
        tool_input: Optional input (not used for this function)
        
    Returns:
        String with comma-separated collection names
    """
    tool_input = tool_input or ""  # Convert None to empty string
    
    try:
        collections = db.list_collection_names()
        return ", ".join(collections)
    except Exception as e:
        traceback.print_exc()
        return f"Error listing collections: {str(e)}"

def infer_schema(collection_name, sample_size=MONGODB_SCHEMA_SAMPLE_SIZE):
    """Infer the schema of a MongoDB collection by sampling documents.
    
    Args:
        collection_name: Name of the collection
        sample_size: Number of documents to sample
        
    Returns:
        Dictionary containing the inferred schema
    """
    try:
        collection = db[collection_name]
        cursor = collection.find().limit(sample_size)
        documents = list(cursor)  # Convert cursor to list synchronously
        
        if not documents:
            return {"message": f"No documents found in collection '{collection_name}'"}
        
        # Sample one document to show as an example
        sample_doc = documents[0]
        
        # Infer schema by analyzing all sampled documents
        schema = {}
        for doc in documents:
            for field, value in doc.items():
                # Skip _id field for schema
                if field == '_id':
                    continue
                    
                field_type = type(value).__name__
                
                if field not in schema:
                    schema[field] = {"type": field_type, "example": value}
                elif schema[field]["type"] != field_type:
                    # Handle mixed types
                    if isinstance(schema[field]["type"], list):
                        if field_type not in schema[field]["type"]:
                            schema[field]["type"].append(field_type)
                    else:
                        schema[field]["type"] = [schema[field]["type"], field_type]
        
        # Format the sample document for better readability
        sample_doc_str = json.dumps(sample_doc, indent=2, cls=MongoJSONEncoder)
        
        return {
            "collection": collection_name,
            "document_count": len(documents),
            "schema": schema,
            "sample_document": sample_doc_str
        }
    except Exception as e:
        traceback.print_exc()
        return {"error": f"Error inferring schema for collection '{collection_name}': {str(e)}"}

def mongo_db_schema(tool_input: str):
    """Given a comma-separated list of collections, returns the schema and sample document for those collections.
    
    Args:
        tool_input: Comma-separated list of collection names
        
    Returns:
        String containing the schema information
    """
    try:
        output = []
        actual_collection_names = [name.strip() for name in tool_input.split(",") if name.strip()]
        
        for collection in actual_collection_names:
            try:
                schema_info = infer_schema(collection)
                
                if "error" in schema_info:
                    output.append(f"Error with collection {collection}: {schema_info['error']}")
                    continue
                
                # Format schema as markdown
                schema_md = f"## Collection: {collection}\n\n"
                schema_md += f"Document count: {schema_info['document_count']}\n\n"
                schema_md += "### Schema:\n\n"
                schema_md += "| Field | Type | Example |\n"
                schema_md += "|-------|------|--------|\n"
                
                for field, info in schema_info["schema"].items():
                    field_type = info["type"]
                    if isinstance(field_type, list):
                        field_type = " or ".join(field_type)
                    
                    # Format example value based on type
                    example = info["example"]
                    if isinstance(example, (dict, list)):
                        example_str = "complex structure"
                    else:
                        example_str = str(example)
                        if len(example_str) > 30:
                            example_str = example_str[:27] + "..."
                    
                    schema_md += f"| {field} | {field_type} | {example_str} |\n"
                
                schema_md += "\n### Sample Document:\n\n"
                schema_md += f"```json\n{schema_info['sample_document']}\n```\n"
                
                output.append(schema_md)
                
            except Exception as e:
                traceback.print_exc()
                output.append(f"Error with collection {collection}: {str(e)}")
        
        return "\n\n".join(output)
    except Exception as e:
        traceback.print_exc()
        return f"General error with schema extraction: {str(e)}"

def mongo_query_checker(query_str):
    """Double-checks if a MongoDB query is correct.
    
    Args:
        query_str: String representation of a MongoDB query
        
    Returns:
        The same query, or a corrected version if mistakes are found
    """
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
        return f"Error checking query: {str(e)}"

def execute_mongo_query(collection_name, filter_query=None, projection=None, sort=None, skip=0, limit=0, pipeline=None): # Added limit=0 default, added pipeline
    """Execute a MongoDB query or aggregation pipeline and return the results.
    
    Args:
        collection_name: Name of the collection to query
        filter_query: MongoDB filter query (default: {})
        projection: MongoDB projection document (default: None)
        sort: MongoDB sort criteria (default: None)
        skip: Number of documents to skip (default: 0)
        limit: Maximum number of documents to return (default: 0 for find, ignored for aggregate if pipeline is used)
        pipeline: MongoDB aggregation pipeline (default: None)
        
    Returns:
        List of documents matching the query or aggregation
    """
    try:
        collection = db[collection_name]
        
        if pipeline:
            # If a pipeline is provided, execute aggregation
            # Limit is typically handled within the pipeline (e.g., $limit stage)
            documents = list(collection.aggregate(pipeline))
        else:
            # Otherwise, execute a find query
            filter_query = filter_query or {}
            cursor = collection.find(filter_query, projection)
            
            if sort:
                cursor = cursor.sort(sort)
            
            cursor = cursor.skip(skip)
            
            # Only apply limit if it's greater than 0 for find operations
            # or if limit is explicitly 0 (meaning no limit for find)
            if limit >= 0: # Allow limit 0 for no limit
                if limit > 0:
                    cursor = cursor.limit(limit)
            # if limit is negative or not set, it implies default behavior (which might be no limit or a server default)
            
            documents = list(cursor)
            
        return documents
    except Exception as e:
        traceback.print_exc()
        raise Exception(f"Error executing MongoDB query/aggregation: {str(e)}")

def mongo_db_query(query_input):
    """Executes a MongoDB query (find or aggregate) and returns the results.
    
    Args:
        query_input: JSON string containing query parameters:
            For find:
            {
                "collection": "collection_name",
                "filter": {}, // MongoDB filter query
                "projection": {}, // Optional: fields to include/exclude
                "sort": {}, // Optional: sort criteria
                "skip": 0, // Optional: number of documents to skip
                "limit": 5 // Optional: maximum number of documents to return (0 for all)
            }
            For aggregate:
            {
                "collection": "collection_name",
                "pipeline": [] // MongoDB aggregation pipeline
            }
            
    Returns:
        String with the query results formatted as a table
    """
    global latest_query_result, latest_query_columns
    
    try:
        query_params = json.loads(query_input)
        collection_name = query_params.get("collection")
        
        if not collection_name:
            return "Error: Collection name is required"

        pipeline = query_params.get("pipeline")

        if pipeline:
            # Aggregation query
            # For aggregations, limit should ideally be part of the pipeline itself ($limit stage)
            # We don't pass a separate limit to execute_mongo_query for pipelines.
            documents = execute_mongo_query(
                collection_name=collection_name,
                pipeline=pipeline
            )
        else:
            # Find query
            filter_query = query_params.get("filter", {})
            projection = query_params.get("projection")
            sort = query_params.get("sort")
            skip = query_params.get("skip", 0)
            # Default limit for find queries if not specified by the LLM in the tool call.
            # The agent's instructions already guide it to use a limit (e.g., 5).
            # If 'limit' is explicitly in query_params, use that value (0 means all).
            # If 'limit' is not in query_params, default to 5 as a safeguard here.
            limit = query_params.get("limit", 5) 

            documents = execute_mongo_query(
                collection_name=collection_name,
                filter_query=filter_query,
                projection=projection,
                sort=sort,
                skip=skip,
                limit=limit
            )
        
        if not documents:
            latest_query_result = None
            latest_query_columns = None
            return "(no documents returned)"
        
        # Format the results as a table
        all_fields = set()
        for doc in documents:
            all_fields.update(doc.keys())
        
        # For find queries, _id is often removed unless projected.
        # For aggregate queries, _id might be the group key, so we keep it by default
        # unless the user's projection in the pipeline explicitly excludes it.
        # The current logic for columns will include _id if it's present.
        if not pipeline and '_id' in all_fields and (not query_params.get("projection") or '_id' not in query_params.get("projection", {})):
            all_fields.remove('_id')
        
        columns = sorted(list(all_fields))
        
        latest_query_result = documents
        latest_query_columns = columns
        
        header = "\t".join(columns)
        rows = []
        for doc in documents:
            row_values = []
            for col in columns:
                val = doc.get(col, "")
                if isinstance(val, (dict, list)):
                    val = json.dumps(val, cls=MongoJSONEncoder)
                elif isinstance(val, ObjectId):
                    val = str(val)
                row_values.append(str(val))
            rows.append("\t".join(row_values))
        
        output = [header] + rows
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

def mongo_count_documents(collection_name, filter_query=None):
    """Count documents in a MongoDB collection that match the filter criteria.
    
    Args:
        collection_name: Name of the collection
        filter_query: MongoDB filter query (default: {})
        
    Returns:
        Number of documents that match the criteria
    """
    try:
        collection = db[collection_name]
        filter_query = filter_query or {}
        count = collection.count_documents(filter_query)
        return str(count)
    except Exception as e:
        traceback.print_exc()
        return f"Error counting documents: {str(e)}"

# --- Visualization Tools ---

def visualize_pie_chart(tool_input: str):
    """
    Generates and displays a pie chart from the latest MongoDB query results.
    The input should be a JSON string specifying the 'labels_column' (for pie chart slices)
    and 'values_column' (for the size of slices).
    If 'values_column' is 'count', it will count occurrences of unique values in 'labels_column'.

    Args:
        tool_input: JSON string with "labels_column" and "values_column".
                    Example: {"labels_column": "department", "values_column": "count"}
                    Example: {"labels_column": "category", "values_column": "total_sales"}
    Returns:
        String confirming chart generation or an error message.
    """
    global latest_query_result
    if latest_query_result is None:
        return "Error: No query has been executed yet. Please run a query first."

    try:
        params = json.loads(tool_input)
        labels_column = params.get("labels_column")
        values_column = params.get("values_column")

        if not labels_column or not values_column:
            return "Error: 'labels_column' and 'values_column' must be specified in the input JSON."

        df = pd.DataFrame(latest_query_result)
        if df.empty:
            return "Error: The last query result was empty. Cannot generate chart."

        if labels_column not in df.columns:
            return f"Error: Labels column '{labels_column}' not found in the query results. Available columns: {', '.join(df.columns)}"

        if values_column == "count":
            if labels_column not in df.columns:
                 return f"Error: Column '{labels_column}' for counting not found in query results."
            counts = df[labels_column].value_counts()
            labels = counts.index
            values = counts.values
            title = f"Pie Chart: Count of {labels_column}"
        elif values_column not in df.columns:
            return f"Error: Values column '{values_column}' not found in the query results. Available columns: {', '.join(df.columns)}"
        else:
            # Aggregate data if multiple rows have the same label
            aggregated_data = df.groupby(labels_column)[values_column].sum()
            labels = aggregated_data.index
            values = aggregated_data.values
            title = f"Pie Chart: {values_column} by {labels_column}"

        plt.figure(figsize=(10, 8))
        plt.pie(values, labels=labels, autopct='%1.1f%%', startangle=140)
        plt.title(title)
        plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
        chart_path = "pie_chart.png"
        plt.savefig(chart_path)
        plt.close()
        return f"Pie chart generated and saved as {chart_path}. Labels: {labels_column}, Values: {values_column}."

    except json.JSONDecodeError:
        return "Error: Invalid JSON input for pie chart."
    except Exception as e:
        traceback.print_exc()
        return f"Error generating pie chart: {str(e)}"

def visualize_bar_chart(tool_input: str):
    """
    Generates and displays a bar chart from the latest MongoDB query results.
    The input should be a JSON string specifying the 'x_column' (for categories on the x-axis)
    and 'y_column' (for values on the y-axis).
    If 'y_column' is 'count', it will count occurrences of unique values in 'x_column'.

    Args:
        tool_input: JSON string with "x_column" and "y_column".
                    Example: {"x_column": "department", "y_column": "count"}
                    Example: {"x_column": "product_name", "y_column": "units_sold"}
    Returns:
        String confirming chart generation or an error message.
    """
    global latest_query_result
    if latest_query_result is None:
        return "Error: No query has been executed yet. Please run a query first."

    try:
        params = json.loads(tool_input)
        x_column = params.get("x_column")
        y_column = params.get("y_column")

        if not x_column or not y_column:
            return "Error: 'x_column' and 'y_column' must be specified in the input JSON."

        df = pd.DataFrame(latest_query_result)
        if df.empty:
            return "Error: The last query result was empty. Cannot generate chart."

        if x_column not in df.columns:
            return f"Error: X-axis column '{x_column}' not found in the query results. Available columns: {', '.join(df.columns)}"

        # Initialize variables to store data for plotting
        x_plot_values = pd.Series(dtype=str) # Initialize as empty Series of strings
        y_plot_values = pd.Series(dtype=float) # Initialize as empty Series of floats
        plot_title = "Bar Chart"
        y_axis_label = y_column

        if y_column == "count":
            if df[x_column].isnull().all():
                return f"Warning: Chart for '{x_column}' vs count would be blank. The column '{x_column}' contains only null/NA values."
            
            counts = df[x_column].value_counts()
            if counts.empty:
                return f"Warning: Chart for '{x_column}' vs count would be blank. No data to plot after counting values in '{x_column}'."
            
            x_plot_values = counts.index.astype(str) # Ensure x-values are strings
            y_plot_values = counts.values
            plot_title = f"Bar Chart: Count of {x_column}"
            y_axis_label = "Count"
        elif y_column in df.columns:
            # Ensure y_column is numeric if we are going to aggregate or plot directly
            if not pd.api.types.is_numeric_dtype(df[y_column]):
                df[y_column] = pd.to_numeric(df[y_column], errors='coerce')
                if df[y_column].isnull().all():
                    return f"Error: Y-axis column '{y_column}' could not be converted to a numeric type or contains only non-numeric/null values."
            
            # Use dropna=False in groupby to include NaN keys if they exist in x_column
            # Check for duplicates in non-NA x_column values to decide on aggregation
            if df[x_column].dropna().duplicated().any():
                aggregated_data = df.groupby(x_column, dropna=False)[y_column].sum().reset_index()
            else:
                aggregated_data = df[[x_column, y_column]].copy()

            if aggregated_data.empty or aggregated_data[y_column].isnull().all():
                return f"Warning: Chart for '{x_column}' vs '{y_column}' would be blank. No valid data after processing (e.g., all y-values are null/NA)."

            x_plot_values = aggregated_data[x_column].astype(str) # Ensure x-values are strings
            y_plot_values = aggregated_data[y_column].fillna(0) # Fill NaN y-values with 0 for plotting
            plot_title = f"Bar Chart: {y_column} by {x_column}"
        else:
            return f"Error: Y-axis column '{y_column}' not found in the query results. Available columns: {', '.join(df.columns)}"

        # Final check before plotting
        if x_plot_values.empty or pd.Series(y_plot_values).isnull().all() or len(y_plot_values) == 0:
             return f"Warning: Chart for '{x_column}' vs '{y_axis_label}' would be blank. No data points to plot after all processing steps."

        plt.figure(figsize=(12, 7))
        plt.bar(x_plot_values, y_plot_values) # x_plot_values is already string type
        plt.xlabel(x_column)
        plt.ylabel(y_axis_label)
        plt.title(plot_title)
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout() # Adjust layout to prevent labels from overlapping
        chart_path = "bar_chart.png"
        plt.savefig(chart_path)
        plt.close() # Close the plot to free up memory
        return f"Bar chart generated and saved as {chart_path}. X-axis: {x_column}, Y-axis: {y_axis_label}."

    except json.JSONDecodeError:
        return "Error: Invalid JSON input for bar chart."
    except Exception as e:
        traceback.print_exc()
        return f"Error generating bar chart: {str(e)}"

# --- Prompt Templates ---
MONGODB_SYSTEM_PROMPT_TEMPLATE = """
You are a MongoDB Assistant that helps users query MongoDB databases using natural language.
Here's the current date and time: {current_datetime}

## Your capabilities:

1. You can list collections in a MongoDB database
2. You can infer and describe the schema of MongoDB collections
3. You can translate natural language questions into MongoDB queries
4. You can execute MongoDB queries and return results
5. You can count documents in collections

## Guidelines:

- Always start by exploring the available collections before answering questions
- Examine collection schemas before generating queries to understand data structure
- Provide clear explanations of your reasoning and how queries work
- Format your answers in a user-friendly way using tables and markdown
- Be precise when discussing MongoDB query operators and syntax
- Include sample MongoDB queries when useful
- Unless specified otherwise, limit results to 5 documents
- Always validate queries before executing them

Remember that MongoDB uses document-based structure, and queries follow BSON format.
"""

# Define MongoDB query template for guiding query generation
MONGODB_QUERY_TEMPLATE = """
When building a MongoDB query:

1. Start with identifying the target collection
2. Construct the filter criteria using appropriate operators:
   - Equality: { field: value }
   - Comparison: { field: { $gt: value } }
   - Array: { field: { $in: [value1, value2] } }
   - Logical: { $and: [ {condition1}, {condition2} ] }
   - Text search: { $text: { $search: "phrase" } }

3. Consider projection to limit returned fields: { field1: 1, field2: 1 }
4. Add sorting when helpful: { field: 1 } for ascending, { field: -1 } for descending
5. Always include skip and limit for pagination

Example query format:
{{
  "collection": "products",
  "filter": {{ "price": {{ "$gt": 100 }} }},
  "projection": {{ "name": 1, "price": 1, "category": 1 }},
  "sort": {{ "price": -1 }},
  "skip": 0,
  "limit": 5
}}
"""

# --- 5. Prepare the tools list ---
tools = [
    mongo_db_list_collections,
    mongo_db_schema,
    mongo_query_checker,
    mongo_db_query,
    mongo_count_documents,
    visualize_pie_chart, # Added tool
    visualize_bar_chart  # Added tool
]

# --- 6. Agent instructions ---
instructions = [
    "You are an agent designed to interact with a MongoDB database and visualize data.",
    "Given an input question, always start by listing the collections in the database.",
    "Then, query the schema of the most relevant collections before writing any MongoDB query.",
    "When you generate a query, double-check it for correctness before executing.",
    "If you get an error executing a query, rewrite and retry.",
    "Unless the user specifies a specific number of documents, always LIMIT results to 5.",
    "Sort results by a relevant field if possible to return the most interesting examples.",
    "Only include relevant fields in your projections to keep responses concise.",
    "Do NOT include database modification operations (insert, update, delete, drop, etc).",
    "Provide clear, concise answers based on the results.",
    "Show your reasoning and used MongoDB queries in your response.",
    "When generating MongoDB queries, follow BSON format and properly format ObjectId references.",
    "Always consider document structure and nested fields when creating queries.",
    "For count operations, use the mongo_count_documents function.",
    "If the user asks for a visualization (e.g., 'show ... as a pie chart', 'generate a bar graph of ...'), use the appropriate visualization tool.",
    "Before calling a visualization tool, ensure a relevant query has been executed and its results are available.",
    "For pie charts, use `visualize_pie_chart` with 'labels_column' and 'values_column'. If 'values_column' is 'count', the tool will count occurrences in 'labels_column'.",
    "For bar charts, use `visualize_bar_chart` with 'x_column' and 'y_column'. If 'y_column' is 'count', the tool will count occurrences in 'x_column'.",
    "Inform the user that the chart has been generated and saved as a file (e.g., 'pie_chart.png' or 'bar_chart.png')."
]

# --- 7. Initialize and run the AGNO Agent ---
try:
    agent = Agent(
        model=Groq(id="llama-3.3-70b-versatile", api_key=groq_api_key),
        tools=tools,
        instructions=instructions + [MONGODB_QUERY_TEMPLATE],  # Add the query template to instructions
        markdown=True,
        show_tool_calls=True,
        add_datetime_to_instructions=True,
    )

    if __name__ == "__main__":
        try:
            print("MongoDB Agentic AI Query Interface started. Type 'exit' or 'quit' to end.")
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