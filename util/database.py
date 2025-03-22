#!/usr/bin/env python3
"""
Database Module

Provides database connectivity and operations for the DataPlor application.
Simplified to focus on core functionality.
"""

import os
import pandas as pd
import duckdb
from datetime import datetime
from typing import Optional
from prefect import task
from contextlib import contextmanager


# Simple logging with print statements

# Database configuration
DATABASE_URL = "md:"  # MotherDuck connection string

# Single connection instance for simple applications
_connection = None

@contextmanager
def get_db_connection(db_path=DATABASE_URL):
    """Context manager for database connections.
    
    Args:
        db_path: Path to the database
        
    Yields:
        Active DuckDB connection
    """
    try:
        # Create a connection to DuckDB
        connection = duckdb.connect(db_path)
        
        # Basic configuration
        connection.execute("PRAGMA memory_limit='2GB'")
        
        # Yield the connection for use
        yield connection
    except Exception as e:
        print(f"Error: Database connection error: {str(e)}")
        raise
    finally:
        # Connections are automatically closed when they go out of scope
        # But we can explicitly close them here if needed
        pass


def is_connection_valid(connection):
    """Check if a connection is valid and usable.
    
    Args:
        connection: DuckDB connection to check
        
    Returns:
        True if connection is valid, False otherwise
    """
    if connection is None:
        return False
        
    try:
        # Try a simple query to test connection
        connection.execute("SELECT 1").fetchone()
        return True
    except Exception as e:
        print(f"Warning: Connection test failed: {str(e)}")
        return False


def get_connection():
    """Get a database connection (initializing if needed).
    
    Returns:
        Active DuckDB connection
    """
    global _connection
    
    # Create a new connection if needed or if the current one is invalid
    if _connection is None or not is_connection_valid(_connection):
        try:
            print(f"Connecting to database: {DATABASE_URL}")
            _connection = duckdb.connect(DATABASE_URL)
        except Exception as e:
            print(f"Error: Error creating connection: {str(e)}")
            raise
            
    return _connection


@task(name="Get Tables", description="Get list of tables from DuckDB")
def get_tables():
    """Get list of tables from DuckDB.
    
    Returns:
        List of table names
    """
    with get_db_connection() as conn:
        try:
            tables_df = conn.execute("SHOW TABLES").fetchdf()
            if not tables_df.empty:
                table_list = tables_df['name'].tolist()
                print(f"Found {len(table_list)} tables in database")
                return table_list
            print("No tables found in database")
            return []
        except Exception as e:
            print(f"Error: Error fetching tables: {str(e)}")
            return []


@task(name="Load Data From Table", description="Load data from an existing DuckDB table")
def load_data_from_table(table_name: str, sample_size: Optional[int] = None):
    """Load data from an existing DuckDB table.
    
    Args:
        table_name: Name of the table to load
        sample_size: If provided, load only this many rows
        
    Returns:
        Tuple of (table_name, DataFrame) or (None, None) if error
    """
    with get_db_connection() as conn:
        try:
            # Construct query with optional sampling
            query = f"SELECT * FROM {table_name}"
            if sample_size is not None and sample_size > 0:
                query += f" LIMIT {sample_size}"
                
            # Execute query
            print(f"Loading data from table: {table_name}")
            df = conn.execute(query).fetchdf()
            print(f"Loaded {len(df)} rows from {table_name}")
            return table_name, df
        except Exception as e:
            print(f"Error: Error loading table {table_name}: {str(e)}")
            return None, None


@task(name="Load Data From File", description="Load data from a file into DuckDB")
def load_data_from_file(file_path: str):
    """Load data from a file into DuckDB.
    
    Args:
        file_path: Path to the file to load
        
    Returns:
        Tuple of (table_name, DataFrame) or (None, None) if error
    """
    # Verify file exists
    if not os.path.exists(file_path):
        print(f"Error: File not found: {file_path}")
        return None, None
        
    # Determine file type and create table name
    file_ext = os.path.splitext(file_path)[1].lower()
    file_name = os.path.basename(file_path).split('.')[0]
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    table_name = f"{file_name}_{timestamp}"
    
    with get_db_connection() as conn:
        try:
            print(f"Loading data from {file_path} into table {table_name}")
            
            if file_ext == '.csv':
                # Use DuckDB's read_csv function
                conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{file_path}')")
            elif file_ext == '.parquet':
                # Use DuckDB's read_parquet function
                conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{file_path}')")
            elif file_ext in ['.xlsx', '.xls']:
                # For Excel files, use pandas and then load into DuckDB
                df = pd.read_excel(file_path)
                conn.register("temp_df", df)
                conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_df")
            else:
                print(f"Error: Unsupported file format: {file_ext}. Please use CSV, Parquet, or Excel files.")
                return None, None
            
            # Fetch the data as a DataFrame
            df = conn.execute(f"SELECT * FROM {table_name}").fetchdf()
            print(f"Loaded {len(df)} rows into {table_name}")
            
            return table_name, df
        
        except Exception as e:
            print(f"Error: Error loading file: {str(e)}")
            return None, None


@task(name="Save Cleaned Data", description="Save cleaned data to a new table in DuckDB")
def save_cleaned_data(df: pd.DataFrame, original_table_name: str):
    """Save cleaned data to a new table in DuckDB.
    
    Args:
        df: DataFrame containing the cleaned data
        original_table_name: Name of the original table
        
    Returns:
        Name of the new table or None if error
    """
    if df is None or df.empty:
        print("Error: Cannot save empty DataFrame")
        return None
    
    # Create a new table name with timestamp
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    new_table_name = f"{original_table_name}_cleaned_{timestamp}"
    
    with get_db_connection() as conn:
        try:
            # Register the DataFrame as a new table
            conn.register("temp_df", df)
            
            # Create a persistent table
            conn.execute(f"CREATE TABLE {new_table_name} AS SELECT * FROM temp_df")
            
            print(f"Saved cleaned data to table {new_table_name} with {len(df)} rows")
            return new_table_name
        except Exception as e:
            print(f"Error: Error saving cleaned data: {str(e)}")
            return None


def close_connection():
    """Close the database connection."""
    global _connection
    
    if _connection is not None:
        try:
            _connection.close()
            print("Closed database connection")
        except Exception as e:
            print(f"Error: Error closing connection: {str(e)}")
        finally:
            _connection = None


def execute_query(query: str):
    """Execute a SQL query and return results as a DataFrame.
    
    Args:
        query: SQL query to execute
        
    Returns:
        DataFrame with results or None if error
    """
    with get_db_connection() as conn:
        try:
            result = conn.execute(query).fetchdf()
            return result
        except Exception as e:
            print(f"Error: Query execution error: {str(e)}")
            return None
