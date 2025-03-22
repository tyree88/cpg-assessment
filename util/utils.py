#!/usr/bin/env python3
"""
Utilities Module

Provides shared utility functions for the dataplor application.
"""

import os
import re
import json
import pandas as pd
from typing import Dict, List, Any, Tuple
from datetime import datetime
import hashlib

# Simple logging with print statements


def validate_dataframe(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    Validate a pandas DataFrame for basic integrity
    
    Args:
        df: Pandas DataFrame to validate
        
    Returns:
        Tuple containing (is_valid, list_of_issues)
    """
    issues = []
    
    # Check if DataFrame is empty
    if df.empty:
        issues.append("DataFrame is empty")
        return False, issues
    
    # Check for minimum required columns for CPG data
    required_columns = ['id', 'name']
    missing_required = [col for col in required_columns if col not in df.columns]
    if missing_required:
        issues.append(f"Missing required columns: {', '.join(missing_required)}")
    
    # Check for all-null columns
    null_columns = [col for col in df.columns if df[col].isnull().all()]
    if null_columns:
        issues.append(f"Columns with all null values: {', '.join(null_columns)}")
    
    # Check for duplicate column names
    if len(df.columns) != len(set(df.columns)):
        issues.append("DataFrame contains duplicate column names")
    
    return len(issues) == 0, issues


def generate_file_hash(file_path: str) -> str:
    """
    Generate a hash for a file to track changes
    
    Args:
        file_path: Path to the file
        
    Returns:
        Hash string for the file
    """
    if not os.path.exists(file_path):
        print(f"Error: File not found: {file_path}")
        return ""
    
    try:
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as e:
        print(f"Error: Error generating hash for {file_path}: {str(e)}")
        return ""


def safe_json_serialize(obj: Any) -> Any:
    """
    Safely serialize objects to JSON, handling non-serializable types
    
    Args:
        obj: Object to serialize
        
    Returns:
        JSON-serializable version of the object
    """
    if isinstance(obj, (datetime, pd.Timestamp)):
        return obj.isoformat()
    elif isinstance(obj, pd.DataFrame):
        return obj.to_dict(orient='records')
    elif isinstance(obj, pd.Series):
        return obj.to_dict()
    elif isinstance(obj, (set, frozenset)):
        return list(obj)
    elif isinstance(obj, (bytes, bytearray)):
        return obj.decode('utf-8', errors='replace')
    elif hasattr(obj, '__dict__'):
        return obj.__dict__
    else:
        return str(obj)


def save_analysis_results(results: Dict[str, Any], output_path: str, filename: str) -> str:
    """
    Save analysis results to a JSON file
    
    Args:
        results: Dictionary of analysis results
        output_path: Directory to save the file
        filename: Base filename without extension
        
    Returns:
        Path to the saved file
    """
    # Ensure output directory exists
    os.makedirs(output_path, exist_ok=True)
    
    # Add timestamp to filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    full_filename = f"{filename}_{timestamp}.json"
    file_path = os.path.join(output_path, full_filename)
    
    try:
        # Convert results to JSON-serializable format
        serializable_results = json.loads(
            json.dumps(results, default=safe_json_serialize)
        )
        
        # Write to file
        with open(file_path, 'w') as f:
            json.dump(serializable_results, f, indent=2)
        
        print(f"Analysis results saved to {file_path}")
        return file_path
    except Exception as e:
        print(f"Error: Error saving analysis results: {str(e)}")
        return ""


def detect_data_types(df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    """
    Detect and categorize data types in a DataFrame
    
    Args:
        df: Pandas DataFrame to analyze
        
    Returns:
        Dictionary mapping column names to their detected data types and properties
    """
    type_info = {}
    
    for column in df.columns:
        col_type = str(df[column].dtype)
        sample = df[column].dropna().head(5).tolist()
        
        # Initialize column info
        type_info[column] = {
            'pandas_type': col_type,
            'inferred_type': 'unknown',
            'sample_values': sample,
            'contains_nulls': df[column].isnull().any()
        }
        
        # Try to infer more specific types
        non_null_values = df[column].dropna()
        
        if len(non_null_values) == 0:
            type_info[column]['inferred_type'] = 'empty'
            continue
            
        # Check if it's a date/time
        if col_type.startswith('datetime'):
            type_info[column]['inferred_type'] = 'datetime'
        elif 'int' in col_type or 'float' in col_type:
            type_info[column]['inferred_type'] = 'numeric'
            type_info[column]['min'] = float(non_null_values.min())
            type_info[column]['max'] = float(non_null_values.max())
            type_info[column]['mean'] = float(non_null_values.mean())
        elif col_type == 'bool':
            type_info[column]['inferred_type'] = 'boolean'
        elif col_type == 'object':
            # Try to detect if it's a date string
            date_pattern = r'^\d{4}[-/]\d{1,2}[-/]\d{1,2}$'
            if non_null_values.astype(str).str.match(date_pattern).any():
                type_info[column]['inferred_type'] = 'date_string'
            
            # Check if it might be a phone number
            phone_pattern = r'^\+?[\d\s\(\)-]{7,20}$'
            if non_null_values.astype(str).str.match(phone_pattern).all():
                type_info[column]['inferred_type'] = 'phone_number'
            
            # Check if it might be an email
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            if non_null_values.astype(str).str.match(email_pattern).all():
                type_info[column]['inferred_type'] = 'email'
                
            # Check if it might be a URL
            url_pattern = r'^https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+'
            if non_null_values.astype(str).str.match(url_pattern).all():
                type_info[column]['inferred_type'] = 'url'
                
            # Check if it might be a categorical variable (few unique values)
            unique_ratio = len(non_null_values.unique()) / len(non_null_values)
            if unique_ratio < 0.1 and len(non_null_values.unique()) < 20:
                type_info[column]['inferred_type'] = 'categorical'
                type_info[column]['categories'] = non_null_values.unique().tolist()[:10]  # First 10 categories
                type_info[column]['category_count'] = len(non_null_values.unique())
            
            # If still unknown, it's probably just text
            if type_info[column]['inferred_type'] == 'unknown':
                type_info[column]['inferred_type'] = 'text'
                
    return type_info


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize column names for consistency
    
    Args:
        df: Pandas DataFrame to process
        
    Returns:
        DataFrame with standardized column names
    """
    # Make a copy to avoid modifying the original
    df_copy = df.copy()
    
    # Create a mapping of old to new column names
    column_mapping = {}
    for col in df_copy.columns:
        # Convert to lowercase
        new_col = col.lower()
        
        # Replace spaces and special characters with underscores
        new_col = re.sub(r'[^a-z0-9]', '_', new_col)
        
        # Remove consecutive underscores
        new_col = re.sub(r'_+', '_', new_col)
        
        # Remove leading and trailing underscores
        new_col = new_col.strip('_')
        
        column_mapping[col] = new_col
    
    # Rename columns
    df_copy.rename(columns=column_mapping, inplace=True)
    
    print(f"Standardized {len(column_mapping)} column names")
    
    return df_copy


def estimate_memory_usage(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Estimate memory usage of a DataFrame
    
    Args:
        df: Pandas DataFrame to analyze
        
    Returns:
        Dictionary with memory usage information
    """
    memory_usage = {
        'total': {
            'bytes': df.memory_usage(deep=True).sum(),
            'mb': df.memory_usage(deep=True).sum() / (1024 * 1024)
        },
        'columns': {}
    }
    
    # Get memory usage by column
    for col in df.columns:
        col_memory = df[col].memory_usage(deep=True)
        memory_usage['columns'][col] = {
            'bytes': col_memory,
            'mb': col_memory / (1024 * 1024),
            'percent': (col_memory / memory_usage['total']['bytes']) * 100
        }
    
    return memory_usage
