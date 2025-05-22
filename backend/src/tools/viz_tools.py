"""
Visualization tools for database query results.
Provides functions for generating various charts and plots.
"""
import seaborn as sns
import matplotlib.pyplot as plt
import json
from src.tools.sql_tools import get_latest_query_results

latest_query_result, latest_query_columns = get_latest_query_results()

def create_pie_chart(data_input):
    """Create and save a pie chart as PNG using seaborn/matplotlib."""
    global latest_query_result, latest_query_columns
    if not latest_query_result or not latest_query_columns:
        return "Error: No query results available. Please run a SQL query first."
    try:
        cfg = json.loads(data_input)
        title = cfg.get('title', 'Pie Chart')
        labels_col = cfg.get('labels_column')
        values_col = cfg.get('values_column')
        if not labels_col or not values_col:
            return f"Error: 'labels_column' and 'values_column' required. Available: {', '.join(latest_query_columns)}"
        idx_l = list(latest_query_columns).index(labels_col)
        idx_v = list(latest_query_columns).index(values_col)
        labels = [row[idx_l] for row in latest_query_result]
        vals = [float(row[idx_v]) for row in latest_query_result]
        plt.figure(figsize=(8,6))
        plt.pie(vals, labels=labels, autopct='%1.1f%%')
        plt.title(title)
        png = 'pie_chart.png'
        plt.savefig(png)
        plt.close()
        return f"Pie chart saved as '{png}'."
    except Exception as e:
        return f"Error creating pie chart: {str(e)}"

def create_bar_chart(data_input):
    """Create and save a bar chart as PNG using seaborn/matplotlib."""
    global latest_query_result, latest_query_columns
    try:
        cfg = json.loads(data_input)
        title = cfg.get('title', 'Bar Chart')
        # If direct arrays provided, use them
        if 'labels' in cfg and 'values' in cfg:
            labels = cfg['labels']
            vals = cfg['values']
            plt.figure(figsize=(10,6))
            sns.barplot(x=labels, y=vals)
            plt.xlabel('')
            plt.ylabel('')
            plt.title(title)
            png = 'bar_chart.png'
            plt.savefig(png)
            plt.close()
            return f"Bar chart saved as '{png}' using provided data."
        # Otherwise use latest query results
        if not latest_query_result or not latest_query_columns:
            return "Error: No query results available. Please run a SQL query first."
        x_col = cfg.get('x_column')
        y_col = cfg.get('y_column')
        if not x_col or not y_col:
            return f"Error: 'x_column' and 'y_column' required. Available: {', '.join(latest_query_columns)}"
        idx_x = list(latest_query_columns).index(x_col)
        idx_y = list(latest_query_columns).index(y_col)
        x = [row[idx_x] for row in latest_query_result]
        y = [float(row[idx_y]) for row in latest_query_result]
        plt.figure(figsize=(10,6))
        sns.barplot(x=x, y=y)
        plt.xlabel(x_col)
        plt.ylabel(y_col)
        plt.title(title)
        png = 'bar_chart.png'
        plt.savefig(png)
        plt.close()
        return f"Bar chart saved as '{png}' using query results."
    except Exception as e:
        return f"Error creating bar chart: {str(e)}"