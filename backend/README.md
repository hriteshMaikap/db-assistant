# Database Assistant

A powerful tool for interacting with your database through SQL queries and visualizations.

## Features

- Connect to MySQL or SQLite databases
- Query the database using SQL
- Visualize results with bar and pie charts
- Modern web interface using Gradio
- FastAPI backend for reliable performance

## Components

- **FastAPI Backend**: RESTful API for database operations
- **SQL Tools**: Core database functionality and visualization
- **Gradio Frontend**: User-friendly web interface

## Setup

1. Install dependencies:
   ```
   pip install -r requirements_updated.txt
   ```

2. Set up environment variables (create a `.env` file):
   ```
   MYSQL_USER=root
   MYSQL_PASS=your_password
   MYSQL_HOST=127.0.0.1
   MYSQL_PORT=3306
   MYSQL_DB=your_database
   GROQ_API_KEY=your_groq_api_key
   API_URL=http://127.0.0.1:8000
   ```

3. Run the FastAPI backend:
   ```
   python main.py
   ```

4. Run the Gradio frontend:
   ```
   python app.py
   ```

5. Open your browser at http://127.0.0.1:7860 to use the application

## Usage

### Database Explorer Tab

- List all tables in the database
- View schema of selected tables
- Write and execute SQL queries
- View results in tabular format

### Visualization Tab

- Create bar charts and pie charts from query results
- Customize chart title and data columns
- View the generated Plotly code

## License

This project is licensed under the MIT License - see the LICENSE file for details.
