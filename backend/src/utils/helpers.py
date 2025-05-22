"""
Helper utilities for the database assistant.
Includes miscellaneous helper functions.
"""
import sys
import traceback

def safe_exit(message, error=None, exit_code=1):
    """Exit the application safely with an error message and optional traceback."""
    print(message)
    if error:
        print(f"Error details: {str(error)}")
        traceback.print_exc()
    sys.exit(exit_code)

def format_table(headers, rows):
    """Format data as a pretty table for console output.
    
    Args:
        headers: List of column headers
        rows: List of data rows
    
    Returns:
        Formatted table string
    """
    if not rows:
        return "(no data)"
        
    # Calculate column widths
    col_widths = [len(str(h)) for h in headers]
    for row in rows:
        for i, val in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(val)))
    
    # Create header
    header = " | ".join(f"{h:{w}s}" for h, w in zip(headers, col_widths))
    separator = "-+-".join("-" * w for w in col_widths)
    
    # Create rows
    formatted_rows = [
        " | ".join(f"{str(val):{w}s}" for val, w in zip(row, col_widths))
        for row in rows
    ]
    
    # Combine all parts
    return "\n".join([header, separator] + formatted_rows)
