"""
Database connection management module.
Handles connecting to MySQL or falling back to SQLite.
"""
import os
import sys
import traceback
from sqlalchemy import create_engine, text, inspect
from src.config.settings import MYSQL_USER, MYSQL_PASS, MYSQL_HOST, MYSQL_PORT, MYSQL_DB, SQLITE_PATH

# Global variables
engine = None
inspector = None

def initialize_connection():
    """Initialize database connection to MySQL or fall back to SQLite."""
    global engine, inspector
    
    print(f"Connecting to MySQL: {MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB} as {MYSQL_USER}")

    try:
        # Attempt MySQL connection
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
        
        # Fall back to SQLite
        try:
            print(f"Looking for SQLite database at: {SQLITE_PATH}")
            
            if os.path.exists(SQLITE_PATH):
                engine = create_engine(f"sqlite:///{SQLITE_PATH}")
                print("SQLite connection successful!")
            else:
                print(f"ERROR: SQLite database file not found at {SQLITE_PATH}")
                sys.exit(1)
        except Exception as e:
            print(f"SQLite connection error: {str(e)}")
            sys.exit(1)

    # Initialize inspector for database metadata
    try:
        inspector = inspect(engine)
        print(f"Database metadata inspection initialized")
    except Exception as e:
        print(f"Error initializing inspector: {str(e)}")
        traceback.print_exc()
        sys.exit(1)
    
    return engine, inspector
