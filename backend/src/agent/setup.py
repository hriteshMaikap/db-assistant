"""
Agent setup and configuration module.
Handles initialization of the Groq agent and its instructions.
"""
from agno.agent import Agent
from agno.models.groq import Groq
from src.config.settings import GROQ_API_KEY, AGENT_MODEL_ID

def setup_agent_tools(sql_tools, database_context):
    """Set up tools for the agent to use."""
    return [
        sql_tools.sql_db_list_tables,
        sql_tools.sql_db_schema,
        sql_tools.sql_db_query_checker,
        sql_tools.sql_db_query,
        sql_tools.get_db_capabilities
    ]

def create_agent_instructions(database_context):
    """Create instructions for the agent."""
    return [
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
        "",
        "Visualization capabilities:",
        "- in development,"
        "",
        "Error handling:",
        "- If a table or column doesn't exist, clearly explain which ones are invalid.",
        "- If a query fails, analyze the error and suggest corrections.",
        "- If the user request is beyond the capability of the database, explain why and suggest alternatives.",
        "",
        f"Complete database context: {database_context}"
    ]

def initialize_agent(database_context, tools):
    """Initialize the AGNO agent with the given database context and tools."""
    try:
        # Create instructions with the given database context
        instructions = create_agent_instructions(database_context)
        
        # Initialize agent
        agent = Agent(
            model=Groq(id=AGENT_MODEL_ID, api_key=GROQ_API_KEY),
            tools=tools,
            instructions=instructions,
            markdown=True,
            show_tool_calls=True,
            add_datetime_to_instructions=True,
        )
        
        return agent
    except Exception as e:
        print(f"Error initializing agent: {str(e)}")
        raise
