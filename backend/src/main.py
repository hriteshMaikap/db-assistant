"""
Main entry point for the Database Assistant application.
Initializes the database connection and agent, and runs the interactive loop.
"""
import traceback
import importlib.util
from src.config.settings import GROQ_API_KEY
from src.database.connection import initialize_connection
from src.database.context import build_database_context
from src.agent.setup import setup_agent_tools, initialize_agent
from src.utils.helpers import safe_exit

def main():
    """Main entry point of the application."""
    # Initialize database connection
    try:
        engine, inspector = initialize_connection()
        if not engine or not inspector:
            safe_exit("Failed to initialize database connection", exit_code=1)
    except Exception as e:
        safe_exit("Error establishing database connection", e, 1)

    # Import modules dynamically to ensure database connection is available
    import src.tools.sql_tools as sql_tools
    
    # Add dependency injection for database connection to sql_tools
    sql_tools_with_connection = create_tools_with_connection(sql_tools, engine, inspector)
    
    # Build database context
    try:
        database_context = build_database_context(engine, inspector)
    except Exception as e:
        safe_exit("Error building database context", e, 1)
    
    # Set up agent with tools
    try:
        tools = setup_agent_tools(sql_tools_with_connection, database_context)
        agent = initialize_agent(database_context, tools)
    except Exception as e:
        safe_exit("Error initializing agent", e, 1)
    
    # Interactive loop
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
                database_context = build_database_context(engine, inspector)
                
                # Re-initialize agent with fresh context
                tools = setup_agent_tools(sql_tools_with_connection, database_context)
                agent = initialize_agent(database_context, tools)
                
                # Get response from agent
                agent.print_response(question, stream=True)
            except Exception as e:
                print(f"Error during agent response: {str(e)}")
                traceback.print_exc()
    except KeyboardInterrupt:
        print("\nExiting due to user interrupt...")
    except Exception as e:
        safe_exit("Error in main loop", e, 1)

def create_tools_with_connection(sql_tools_module, engine, inspector):
    """Create a wrapper class for sql_tools that injects engine and inspector parameters."""
    class SqlToolsWithConnection:
        def sql_db_list_tables(self, tool_input=None):
            return sql_tools_module.sql_db_list_tables(inspector, tool_input)
        
        def sql_db_schema(self, table_names):
            return sql_tools_module.sql_db_schema(engine, inspector, table_names)
        
        def sql_db_query_checker(self, query):
            return sql_tools_module.sql_db_query_checker(engine, inspector, query)
        
        def sql_db_query(self, query):
            return sql_tools_module.sql_db_query(engine, inspector, query)
        
        def get_db_capabilities(self):
            return sql_tools_module.get_db_capabilities(engine)
    
    return SqlToolsWithConnection()

if __name__ == "__main__":
    main()
