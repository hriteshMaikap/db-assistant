"""
Configuration settings for the database assistant application.
Contains environment variables and database connection parameters.
"""
import os
from dotenv import load_dotenv

# Load environment
load_dotenv()

# API Keys
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
if not GROQ_API_KEY:
    raise ValueError("GROQ_API_KEY not set in .env")

# MySQL connection params
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASS = os.getenv("MYSQL_PASS", "hritesh12345")
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_DB   = os.getenv("MYSQL_DB",   "sales_db")

# SQLite fallback path
SQLITE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "Chinook.db")

# Agent settings
AGENT_MODEL_ID = "llama-3.1-8b-instant"
