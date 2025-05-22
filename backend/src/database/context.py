"""
Database context creation module.
Builds comprehensive context with schema and sample data for all tables.
"""
import traceback
from sqlalchemy import text

# Global variables
database_context = None  # Will store complete DB schema and sample data

def build_database_context(engine, inspector):
    """Build comprehensive database context with schema and sample data for all tables."""
    global database_context
    
    try:
        tables = inspector.get_table_names()
        context_parts = []
        
        for table in tables:
            try:
                # Get schema
                columns = inspector.get_columns(table)
                col_lines = [
                    f"{col['name']} {col['type']}{' PRIMARY KEY' if col.get('primary_key') else ''}"
                    for col in columns
                ]
                create_sql = f"CREATE TABLE {table} (\n    " + ",\n    ".join(col_lines) + "\n)"
                
                # Get sample rows
                with engine.connect() as conn:
                    sample = conn.execute(text(f"SELECT * FROM {table} LIMIT 3")).fetchall()
                    sample_str = ""
                    if sample:
                        colnames = [c["name"] for c in columns]
                        sample_str += "SAMPLE DATA:\n"
                        sample_str += "\t".join(colnames) + "\n"
                        for row in sample:
                            sample_str += "\t".join(str(val) for val in row) + "\n"
                    else:
                        sample_str += "(no data in table)\n"
                
                context_parts.append(f"TABLE: {table}\n{create_sql}\n{sample_str}")
            except Exception as e:
                print(f"Error building context for table {table}: {str(e)}")
                context_parts.append(f"TABLE: {table} - Error retrieving schema: {str(e)}")
        
        database_context = "\n\n".join(context_parts)
        print(f"Database context built successfully with {len(tables)} tables")
        return database_context
    except Exception as e:
        print(f"Error building database context: {str(e)}")
        database_context = f"Error building database context: {str(e)}"
        return database_context
