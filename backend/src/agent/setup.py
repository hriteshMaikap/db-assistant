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
        sql_tools.get_db_capabilities,
        sql_tools.viz_tools.create_pie_chart,
        sql_tools.viz_tools.create_bar_chart,
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
        "The reasoning or the thought process behind the query.",
        "-A small summary in 2 lines about the query result.",
        "-The output from the SQL query in a table format.",
        """
        Here is a sample output format:
        Question: Total sales per category 
        Answer:
        <Reasoning>To determine total sales per category, we need to follow these steps:                                                                                                                                          
            1. List all tables in the database.                                                                      
            2. Take schema of all tables and check schema of tables relevant to the user query.                      
            3. Write SQL queries that relate ONLY to tables and columns that actually exist in the database.         
            4. Validate the SQL query using sql_db_query_checker before executing.                                   
            5. Execute the query with sql_db_query. 
        
        <The Query>    
        To calculate the total sales per category, we need to join the order_items, products, and categories     
        tables. Here's a SQL query that does this:                                                               
                                                                                                            
        SELECT c.name, SUM(oi.quantity * p.price) as total_sales                                                
        FROM order_items oi                                                                                     
        JOIN products p ON oi.product_id = p.id                                                                 
        JOIN categories c ON p.category_id = c.id                                                               
        GROUP BY c.name                                                                                         
        ORDER BY total_sales DESC                                                                               
        LIMIT 5;

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
            reasoning=True,
            markdown=True,
            show_tool_calls=True,
            add_datetime_to_instructions=True,
        )
        
        return agent
    except Exception as e:
        print(f"Error initializing agent: {str(e)}")
        raise
