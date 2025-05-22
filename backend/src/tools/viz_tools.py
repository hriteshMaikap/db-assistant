"""
Visualization tools for database query results.
Provides functions for generating various charts and plots.
"""
import datetime
import traceback
import pandas as pd
import plotly.express as px
from src.tools.sql_tools import get_latest_query_results

def generate_plot(plot_type, x_column, y_column=None, title=None):
    """Generate a Plotly visualization from the latest query results.
    
    Args:
        plot_type: Type of plot ('bar', 'pie', 'line', 'scatter')
        x_column: Column to use for the x-axis
        y_column: Column to use for the y-axis (optional for some plots)
        title: Custom title for the plot (optional)
    
    Returns:
        String with result message and Plotly code
    """
    latest_query_result, latest_query_columns = get_latest_query_results()
    
    try:
        if latest_query_result is None or latest_query_columns is None:
            return "No query results available for visualization. Please run a query first."
        
        # Convert query results to DataFrame
        df = pd.DataFrame(latest_query_result, columns=latest_query_columns)
        
        # Check if specified columns exist
        if x_column not in df.columns:
            available_cols = ", ".join(df.columns)
            return f"Error: Column '{x_column}' not found in query results. Available columns: {available_cols}"
        
        if y_column is not None and y_column not in df.columns:
            available_cols = ", ".join(df.columns)
            return f"Error: Column '{y_column}' not found in query results. Available columns: {available_cols}"
        
        # Generate timestamp for file naming
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"results_{timestamp}.png"
        
        # Create appropriate visualization
        if plot_type.lower() == 'bar':
            if y_column:
                # Make sure y_column is numeric
                if pd.api.types.is_numeric_dtype(df[y_column]) or df[y_column].apply(lambda x: str(x).replace('.', '').isdigit()).all():
                    df[y_column] = pd.to_numeric(df[y_column], errors='coerce')
                    fig = px.bar(df, x=x_column, y=y_column, title=title or f"Bar Chart: {x_column} vs {y_column}")
                else:
                    # Count occurrences if y_column is not numeric
                    counts = df.groupby(x_column).size().reset_index(name='count')
                    fig = px.bar(counts, x=x_column, y='count', title=title or f"Bar Chart: Count of {x_column}")
            else:
                # Count occurrences if no y_column provided
                counts = df.groupby(x_column).size().reset_index(name='count')
                fig = px.bar(counts, x=x_column, y='count', title=title or f"Bar Chart: Count of {x_column}")
        
        elif plot_type.lower() == 'pie':
            if y_column and (pd.api.types.is_numeric_dtype(df[y_column]) or df[y_column].apply(lambda x: str(x).replace('.', '').isdigit()).all()):
                df[y_column] = pd.to_numeric(df[y_column], errors='coerce')
                # Group by x_column and sum y_column values
                grouped = df.groupby(x_column)[y_column].sum().reset_index()
                fig = px.pie(grouped, names=x_column, values=y_column, title=title or f"Pie Chart: {y_column} by {x_column}")
            else:
                # Count occurrences for pie chart
                counts = df[x_column].value_counts().reset_index()
                counts.columns = [x_column, 'count']
                fig = px.pie(counts, names=x_column, values='count', title=title or f"Pie Chart: Distribution of {x_column}")
        
        elif plot_type.lower() == 'line':
            if not y_column:
                return "Error: Line charts require both x_column and y_column parameters."
                
            # Make sure y_column is numeric
            if pd.api.types.is_numeric_dtype(df[y_column]) or df[y_column].apply(lambda x: str(x).replace('.', '').isdigit()).all():
                df[y_column] = pd.to_numeric(df[y_column], errors='coerce')
                fig = px.line(df, x=x_column, y=y_column, title=title or f"Line Chart: {x_column} vs {y_column}")
            else:
                return f"Error: Column '{y_column}' must contain numeric values for line chart."
        
        elif plot_type.lower() == 'scatter':
            if not y_column:
                return "Error: Scatter plots require both x_column and y_column parameters."
                
            # Make sure y_column is numeric
            if pd.api.types.is_numeric_dtype(df[y_column]) or df[y_column].apply(lambda x: str(x).replace('.', '').isdigit()).all():
                df[y_column] = pd.to_numeric(df[y_column], errors='coerce')
                fig = px.scatter(df, x=x_column, y=y_column, title=title or f"Scatter Plot: {x_column} vs {y_column}")
            else:
                return f"Error: Column '{y_column}' must contain numeric values for scatter plot."
        
        else:
            return f"Unsupported plot type: {plot_type}. Supported types are 'bar', 'pie', 'line', and 'scatter'."
        
        # Save the figure
        try:
            fig.write_image(filename)
            # Show the figure in a new window
            fig.show()
            
            # Return Plotly code for the user
            plotly_code = generate_plotly_code(plot_type, x_column, y_column, title)
            
            return f"Visualization created and saved as '{filename}'.\n\nHere's the Plotly code to recreate this chart:\n\n```python\n{plotly_code}```"
        except Exception as e:
            error_msg = f"Error saving/displaying visualization: {str(e)}"
            print(error_msg)
            traceback.print_exc()
            
            # Return just the code if saving/showing fails
            plotly_code = generate_plotly_code(plot_type, x_column, y_column, title)
            return f"Error saving visualization: {str(e)}\n\nHere's the Plotly code to create this chart:\n\n```python\n{plotly_code}```"
    
    except Exception as e:
        error_msg = f"Error generating visualization: {str(e)}"
        print(error_msg)
        traceback.print_exc()
        return error_msg

def generate_plotly_code(plot_type, x_column, y_column=None, title=None):
    """Generates Python code for recreating the Plotly visualization."""
    
    code = [
        "import pandas as pd",
        "import plotly.express as px",
        "",
        "# Assuming df is your DataFrame with the query results",
        ""
    ]
    
    if plot_type.lower() == 'bar':
        if y_column:
            code.append(f"# Create bar chart of {x_column} vs {y_column}")
            code.append(f"fig = px.bar(df, x='{x_column}', y='{y_column}', title='{title or f'Bar Chart: {x_column} vs {y_column}'}')")
        else:
            code.append(f"# Count occurrences of {x_column}")
            code.append(f"counts = df.groupby('{x_column}').size().reset_index(name='count')")
            code.append(f"fig = px.bar(counts, x='{x_column}', y='count', title='{title or f'Bar Chart: Count of {x_column}'}')")
    
    elif plot_type.lower() == 'pie':
        if y_column:
            code.append(f"# Create pie chart showing {y_column} distribution by {x_column}")
            code.append(f"grouped = df.groupby('{x_column}')['{y_column}'].sum().reset_index()")
            code.append(f"fig = px.pie(grouped, names='{x_column}', values='{y_column}', title='{title or f'Pie Chart: {y_column} by {x_column}'}')")
        else:
            code.append(f"# Create pie chart showing distribution of {x_column}")
            code.append(f"counts = df['{x_column}'].value_counts().reset_index()")
            code.append(f"counts.columns = ['{x_column}', 'count']")
            code.append(f"fig = px.pie(counts, names='{x_column}', values='count', title='{title or f'Pie Chart: Distribution of {x_column}'}')")
    
    elif plot_type.lower() == 'line':
        code.append(f"# Create line chart of {x_column} vs {y_column}")
        code.append(f"fig = px.line(df, x='{x_column}', y='{y_column}', title='{title or f'Line Chart: {x_column} vs {y_column}'}')")
    
    elif plot_type.lower() == 'scatter':
        code.append(f"# Create scatter plot of {x_column} vs {y_column}")
        code.append(f"fig = px.scatter(df, x='{x_column}', y='{y_column}', title='{title or f'Scatter Plot: {x_column} vs {y_column}'}')")
    
    code.extend([
        "",
        "# Display the figure",
        "fig.show()",
        "",
        "# Save the figure",
        "fig.write_image('chart.png')"
    ])
    
    return "\n".join(code)
