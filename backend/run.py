#!/usr/bin/env python
"""
Wrapper script to launch the Database Assistant application.
"""
import sys
import os

# Add the backend directory to the Python path
backend_path = os.path.dirname(os.path.abspath(__file__))
if backend_path not in sys.path:
    sys.path.append(backend_path)

# Import and run the main function
from src.main import main

if __name__ == "__main__":
    main()
