import streamlit as st
import pandas as pd
import duckdb
from datetime import datetime

# Connect to MotherDuck database
con = duckdb.connect('md:my_db')

def get_tables():
    """Get list of tables from DuckDB"""
    try:
        tables_df = con.execute("SHOW TABLES").fetchdf()
        if not tables_df.empty:
            return tables_df['name'].tolist()
        return []
    except Exception as e:
        st.error(f"Error fetching tables: {str(e)}")
        return []

def load_data_from_table(table_name):
    """Load data from an existing DuckDB table"""
    try:
        # Use DuckDB's SQL interface to fetch the data
        df = con.execute(f"SELECT * FROM {table_name}").fetchdf()
        return table_name, df
    except Exception as e:
        st.error(f"Error loading table {table_name}: {str(e)}")
        return None, None

def load_data_from_file(file_path):
    """Load data from a file into DuckDB"""
    try:
        # Determine file type and use appropriate DuckDB method
        if file_path.endswith('.csv'):
            # Use DuckDB's read_csv function
            table_name = f"temp_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{file_path}')")
            df = con.execute(f"SELECT * FROM {table_name}").fetchdf()
            return table_name, df
        
        elif file_path.endswith('.parquet'):
            # Use DuckDB's read_parquet function
            table_name = f"temp_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{file_path}')")
            df = con.execute(f"SELECT * FROM {table_name}").fetchdf()
            return table_name, df
        
        elif file_path.endswith('.xlsx'):
            # For Excel files, use pandas and then load into DuckDB
            df = pd.read_excel(file_path)
            table_name = f"temp_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
            return table_name, df
        
        else:
            st.error("Unsupported file format. Please use CSV, Parquet, or Excel files.")
            return None, None
    
    except Exception as e:
        st.error(f"Error loading file: {str(e)}")
        return None, None

def save_cleaned_data(df, original_table_name):
    """Save cleaned data to a new table in DuckDB"""
    try:
        # Create a new table name based on the original
        cleaned_table = f"{original_table_name}_cleaned"
        
        # Register the DataFrame with DuckDB and create a table
        con.register("temp_df", df)
        con.execute(f"DROP TABLE IF EXISTS {cleaned_table}")
        con.execute(f"CREATE TABLE {cleaned_table} AS SELECT * FROM temp_df")
        
        st.success(f"Cleaned data saved to table: {cleaned_table}")
        return cleaned_table
    except Exception as e:
        st.error(f"Error saving cleaned data: {str(e)}")
        return None
