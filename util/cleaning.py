import pandas as pd
import streamlit as st
import duckdb
import json
import re
from prefect import task

# Connect to MotherDuck database
con = duckdb.connect('md:my_db')

@task(name="Identify Data Quality Issues for Cleaning", description="Identify data quality issues for cleaning", tags=["data-quality", "cleaning"])
def identify_data_quality_issues(df, analysis, table_name):
    """Identify data quality issues with a focus on CPG and point-of-interest data"""
    issues = {}
    
    # Missing values - critical for CPG data
    if analysis['missing_values']:
        issues['missing_values'] = analysis['missing_values']
        
        # Identify critical missing fields for CPG/POI data
        critical_fields = [col for col in df.columns if any(term in col.lower() for term in 
                                                          ['address', 'location', 'gps', 'lat', 'lon', 'lng', 'coord',
                                                           'product', 'sku', 'upc', 'ean', 'brand', 'category', 'price',
                                                           'store', 'outlet', 'chain', 'retailer'])]
        critical_missing = {col: analysis['missing_values'][col] for col in analysis['missing_values'] 
                          if col in critical_fields}
        
        if critical_missing:
            issues['critical_missing_fields'] = critical_missing
    
    # Duplicate rows - problematic for inventory and store data
    if analysis.get('duplicate_rows', {}).get('count', 0) > 0:
        issues['duplicate_rows'] = analysis['duplicate_rows']
        
        # Try to identify potential primary key candidates
        potential_keys = analysis.get('potential_key_columns', [])
        
        if potential_keys:
            issues['potential_primary_keys'] = potential_keys
    
    # Inconsistent data types
    type_issues = {}
    
    # CPG-specific type checks
    for col in df.columns:
        # Check for numeric columns that might be categorical
        if pd.api.types.is_numeric_dtype(df[col].dtype):
            # Calculate unique count directly
            unique_count = df[col].nunique()
            if 1 < unique_count < 15:  # Small number of unique values suggests categorical
                type_issues[col] = f"Possibly categorical column stored as {df[col].dtype}"
            
            # Check for potential ID columns stored as numeric
            if any(term in col.lower() for term in ['id', 'code', 'sku', 'upc', 'ean', 'gtin']):
                # Check if values have leading zeros when converted to string
                sample = df[col].dropna().astype(str).iloc[0] if not df[col].dropna().empty else ''
                if sample.startswith('0'):
                    type_issues[col] = f"Possible ID with leading zeros stored as {df[col].dtype}"
        
        # Check for date columns stored as strings
        if pd.api.types.is_string_dtype(df[col].dtype):
            # Date detection
            if any(keyword in col.lower() for keyword in ['date', 'time', 'day', 'month', 'year']):
                try:
                    # Get a sample value and ensure it's a string
                    if not df[col].dropna().empty:
                        date_sample = str(df[col].dropna().iloc[0])
                    else:
                        date_sample = ''
                        
                    # Check if the sample matches common date patterns
                    if re.search(r'\d{4}-\d{2}-\d{2}', date_sample) or re.search(r'\d{2}/\d{2}/\d{4}', date_sample):
                        type_issues[col] = "Possibly date column stored as string"
                except Exception:
                    # Skip this check if there are any issues
                    pass
            
            # Check for potential numeric data stored as strings
            try:
                if not any(keyword in col.lower() for keyword in ['name', 'description', 'address', 'city', 'state']):
                    numeric_ratio = df[col].dropna().str.replace('.', '', regex=False).str.isdigit().mean()
                    if numeric_ratio > 0.8:  # If more than 80% of values are numeric
                        type_issues[col] = "Possibly numeric data stored as string"
            except Exception:
                # Skip this check if there are any issues
                pass
    
    if type_issues:
        issues['type_issues'] = type_issues
    
    # Outlier detection for numeric columns
    outliers = {}
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col].dtype):
            # Skip ID-like columns
            if any(term in col.lower() for term in ['id', 'code', 'sku', 'upc', 'ean']):
                continue
                
            # Use DuckDB to detect outliers with SQL
            try:
                stats_query = f"""
                WITH stats AS (
                    SELECT 
                        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY \"{col}\") AS q1,
                        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY \"{col}\") AS q3
                    FROM {table_name}
                    WHERE \"{col}\" IS NOT NULL
                )
                SELECT 
                    q1, 
                    q3,
                    (SELECT COUNT(*) FROM {table_name} 
                     WHERE \"{col}\" < q1 - 1.5 * (q3 - q1) OR \"{col}\" > q3 + 1.5 * (q3 - q1)) AS outlier_count
                FROM stats
                """
                
                result = con.execute(stats_query).fetchone()
                if result and result[2] > 0:
                    outliers[col] = int(result[2])
                    
                    # For price and quantity fields, calculate potential revenue impact
                    if any(term in col.lower() for term in ['price', 'cost', 'amount', 'quantity', 'qty', 'volume']):
                        impact_query = f"""
                        WITH stats AS (
                            SELECT 
                                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY \"{col}\") AS q1,
                                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY \"{col}\") AS q3
                            FROM {table_name}
                            WHERE \"{col}\" IS NOT NULL
                        )
                        SELECT 
                            MIN(\"{col}\") as min_val,
                            MAX(\"{col}\") as max_val,
                            AVG(\"{col}\") as avg_val,
                            SUM(\"{col}\") as total_val
                        FROM {table_name}
                        WHERE \"{col}\" < (SELECT q1 - 1.5 * (q3 - q1) FROM stats) 
                           OR \"{col}\" > (SELECT q3 + 1.5 * (q3 - q1) FROM stats)
                        """
                        
                        impact = con.execute(impact_query).fetchone()
                        if impact:
                            outliers[f"{col}_impact"] = {
                                "min": float(impact[0]) if impact[0] is not None else None,
                                "max": float(impact[1]) if impact[1] is not None else None,
                                "mean": float(impact[2]) if impact[2] is not None else None,
                                "total": float(impact[3]) if impact[3] is not None else None
                            }
            except Exception:
                # Fallback to pandas for outlier detection
                try:
                    Q1 = df[col].quantile(0.25)
                    Q3 = df[col].quantile(0.75)
                    IQR = Q3 - Q1
                    lower_bound = Q1 - 1.5 * IQR
                    upper_bound = Q3 + 1.5 * IQR
                    outlier_count = ((df[col] < lower_bound) | (df[col] > upper_bound)).sum()
                    
                    if outlier_count > 0:
                        outliers[col] = int(outlier_count)
                except Exception:
                    pass
    
    if outliers:
        issues['outliers'] = outliers
    
    # Check for JSON columns that might need parsing
    json_columns = {}
    for col in df.columns:
        if pd.api.types.is_string_dtype(df[col].dtype):
            # Sample the first non-null value
            sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else ''
            # Check if sample is a string before calling string methods
            if isinstance(sample, str) and sample.startswith('{') and sample.endswith('}'):
                try:
                    json.loads(sample)
                    json_columns[col] = "JSON data stored as string"
                except Exception:
                    pass
    
    if json_columns:
        issues['json_columns'] = json_columns
    
    # Check for inconsistent category hierarchies (common in CPG data)
    category_issues = {}
    category_columns = [col for col in df.columns if any(term in col.lower() 
                                                         for term in ['category', 'segment', 'department', 'class'])]
    
    if len(category_columns) > 1:
        # Use DuckDB to check for hierarchical consistency
        for i, parent_col in enumerate(category_columns[:-1]):
            for child_col in category_columns[i+1:]:
                try:
                    hierarchy_query = f"""
                    WITH parent_child_map AS (
                        SELECT \"{parent_col}\", COUNT(DISTINCT \"{child_col}\") as child_count
                        FROM {table_name}
                        WHERE \"{parent_col}\" IS NOT NULL AND \"{child_col}\" IS NOT NULL
                        GROUP BY \"{parent_col}\"
                        HAVING COUNT(DISTINCT \"{child_col}\") > 1
                    )
                    SELECT \"{parent_col}\" as parent, child_count
                    FROM parent_child_map
                    LIMIT 10
                    """
                    
                    inconsistent_parents = con.execute(hierarchy_query).fetchdf()
                    if not inconsistent_parents.empty:
                        parent_list = inconsistent_parents[parent_col].tolist()
                        if parent_list:
                            category_issues[f"{parent_col}_to_{child_col}"] = {
                                "inconsistent_parents": parent_list,
                                "count": len(parent_list)
                            }
                except Exception:
                    pass
    
    if category_issues:
        issues['category_hierarchy_issues'] = category_issues
    
    # Check for potential address/location quality issues
    location_issues = {}
    
    # Identify potential address/location columns
    address_columns = [col for col in df.columns if any(term in col.lower() 
                                                      for term in ['address', 'street', 'city', 'state', 'zip', 'postal'])]
    geo_columns = [col for col in df.columns if any(term in col.lower() 
                                                  for term in ['lat', 'lon', 'lng', 'latitude', 'longitude', 'coord'])]
    
    # Check address completeness
    if address_columns:
        for col in address_columns:
            if pd.api.types.is_string_dtype(df[col].dtype):
                # Check for empty but not null addresses using DuckDB
                try:
                    empty_query = f"SELECT COUNT(*) FROM {table_name} WHERE \"{col}\" = ''"
                    empty_count = con.execute(empty_query).fetchone()[0]
                    if empty_count > 0:
                        location_issues[f"{col}_empty"] = int(empty_count)
                    
                    # Check for potentially invalid addresses (too short)
                    short_addr_query = f"SELECT COUNT(*) FROM {table_name} WHERE LENGTH(\"{col}\") < 8 AND \"{col}\" != ''"
                    short_addr_count = con.execute(short_addr_query).fetchone()[0]
                    if short_addr_count > 0:
                        location_issues[f"{col}_too_short"] = int(short_addr_count)
                except Exception:
                    # Fallback to pandas
                    empty_count = (df[col].str.strip() == '').sum()
                    if empty_count > 0:
                        location_issues[f"{col}_empty"] = int(empty_count)
                    
                    short_addr_count = (df[col].str.len() < 8).sum()
                    if short_addr_count > 0:
                        location_issues[f"{col}_too_short"] = int(short_addr_count)
    
    # Check for missing geo coordinates when address is present
    if address_columns and geo_columns:
        for geo_col in geo_columns:
            try:
                missing_geo_query = f"""
                SELECT COUNT(*) FROM {table_name} 
                WHERE \"{address_columns[0]}\" IS NOT NULL 
                AND \"{address_columns[0]}\" != '' 
                AND \"{geo_col}\" IS NULL
                """
                missing_geo_count = con.execute(missing_geo_query).fetchone()[0]
                if missing_geo_count > 0:
                    location_issues[f"missing_{geo_col}_with_address"] = int(missing_geo_count)
            except Exception:
                # Fallback to pandas
                address_present = df[address_columns[0]].notna() & (df[address_columns[0]] != '')
                geo_missing = df[geo_col].isna()
                missing_geo_count = (address_present & geo_missing).sum()
                if missing_geo_count > 0:
                    location_issues[f"missing_{geo_col}_with_address"] = int(missing_geo_count)
    
    if location_issues:
        issues['location_data_issues'] = location_issues
    
    return issues

@task(name="Generate Cleaning Recommendations", description="Generate recommendations for cleaning data based on identified issues", tags=["data-quality", "cleaning"])
def generate_cleaning_recommendations(df, issues, table_name):
    """Generate recommendations for cleaning data based on identified issues"""
    recommendations = []
    
    # Missing values recommendations
    if 'missing_values' in issues:
        recommendations.append({
            "issue": "Missing Values",
            "description": f"Found {len(issues['missing_values'])} columns with missing values",
            "actions": [
                "Fill missing values with appropriate defaults",
                "Remove rows with critical missing values",
                "Impute missing values based on other columns"
            ]
        })
        
        # Special recommendations for critical missing fields
        if 'critical_missing_fields' in issues:
            recommendations.append({
                "issue": "Critical Missing Fields",
                "description": f"Found {len(issues['critical_missing_fields'])} critical columns with missing values",
                "actions": [
                    "Prioritize filling these critical fields",
                    "Consider removing rows with missing critical data",
                    "Flag records with missing critical data for follow-up"
                ]
            })
    
    # Duplicate rows recommendations
    if 'duplicate_rows' in issues:
        recommendations.append({
            "issue": "Duplicate Rows",
            "description": f"Found {issues['duplicate_rows']} duplicate rows",
            "actions": [
                "Remove duplicate rows",
                "Identify why duplicates exist in the source system",
                "Implement unique constraints in the database"
            ]
        })
        
        if 'potential_primary_keys' in issues:
            recommendations.append({
                "issue": "Missing Primary Key",
                "description": f"Consider using one of these columns as a primary key: {', '.join(issues['potential_primary_keys'][:3])}",
                "actions": [
                    "Establish a proper primary key",
                    "Add a surrogate key if no natural key exists",
                    "Ensure referential integrity with related tables"
                ]
            })
    
    # Data type recommendations
    if 'type_issues' in issues:
        recommendations.append({
            "issue": "Data Type Issues",
            "description": f"Found {len(issues['type_issues'])} columns with potential data type issues",
            "actions": [
                "Convert string dates to proper date types",
                "Convert numeric strings to proper numeric types",
                "Ensure categorical data is properly encoded"
            ]
        })
    
    # Outlier recommendations
    if 'outliers' in issues:
        recommendations.append({
            "issue": "Outliers",
            "description": f"Found outliers in {len([k for k in issues['outliers'] if not k.endswith('_impact')])} numeric columns",
            "actions": [
                "Cap outliers at a reasonable threshold",
                "Remove extreme outliers",
                "Investigate source of outliers"
            ]
        })
        
        # Special recommendations for price/quantity outliers
        price_outliers = [k for k in issues['outliers'] if k.endswith('_impact')]
        if price_outliers:
            recommendations.append({
                "issue": "Price/Quantity Outliers",
                "description": f"Found outliers in {len(price_outliers)} price or quantity columns with potential business impact",
                "actions": [
                    "Review extreme price/quantity values",
                    "Assess revenue impact of outliers",
                    "Implement validation rules for future data entry"
                ]
            })
    
    # JSON column recommendations
    if 'json_columns' in issues:
        recommendations.append({
            "issue": "JSON Data in Strings",
            "description": f"Found {len(issues['json_columns'])} columns with JSON data stored as strings",
            "actions": [
                "Parse JSON data into proper columns",
                "Extract key JSON attributes to separate columns",
                "Consider using a database with native JSON support"
            ]
        })
    
    # Category hierarchy recommendations
    if 'category_hierarchy_issues' in issues:
        recommendations.append({
            "issue": "Inconsistent Category Hierarchies",
            "description": f"Found {len(issues['category_hierarchy_issues'])} inconsistent category relationships",
            "actions": [
                "Standardize category hierarchies",
                "Create a reference table for valid category relationships",
                "Fix inconsistent category assignments"
            ]
        })
    
    # Location data recommendations
    if 'location_data_issues' in issues:
        recommendations.append({
            "issue": "Location Data Issues",
            "description": "Found issues with location/address data",
            "actions": [
                "Standardize address formats",
                "Fill in missing geographic coordinates",
                "Validate addresses against a reference database"
            ]
        })
    
    return recommendations

@task(name="Clean Data", description="Apply cleaning steps to the data", tags=["cleaning"])
def clean_data(df, table_name, cleaning_steps):
    """Apply cleaning steps to the data"""
    # Create a copy of the dataframe to avoid modifying the original
    cleaned_df = df.copy()
    
    # Track changes made
    changes = []
    
    # Apply each cleaning step
    for step in cleaning_steps:
        step_type = step.get('type')
        
        if step_type == 'fill_missing':
            col = step.get('column')
            value = step.get('value')
            if col and value is not None:
                # Use DuckDB to update the values
                try:
                    # First, update our pandas DataFrame
                    null_count_before = cleaned_df[col].isna().sum()
                    cleaned_df[col] = cleaned_df[col].fillna(value)
                    null_count_after = cleaned_df[col].isna().sum()
                    
                    changes.append({
                        "step": "Fill Missing Values",
                        "column": col,
                        "value": str(value),
                        "rows_affected": int(null_count_before - null_count_after)
                    })
                except Exception as e:
                    st.error(f"Error filling missing values in {col}: {str(e)}")
        
        elif step_type == 'remove_duplicates':
            # Use DuckDB to remove duplicates
            try:
                rows_before = len(cleaned_df)
                cleaned_df = cleaned_df.drop_duplicates()
                rows_after = len(cleaned_df)
                
                changes.append({
                    "step": "Remove Duplicates",
                    "rows_affected": int(rows_before - rows_after)
                })
            except Exception as e:
                st.error(f"Error removing duplicates: {str(e)}")
        
        elif step_type == 'convert_type':
            col = step.get('column')
            target_type = step.get('target_type')
            if col and target_type:
                try:
                    if target_type == 'date':
                        cleaned_df[col] = pd.to_datetime(cleaned_df[col], errors='coerce')
                    elif target_type == 'numeric':
                        cleaned_df[col] = pd.to_numeric(cleaned_df[col], errors='coerce')
                    elif target_type == 'categorical':
                        cleaned_df[col] = cleaned_df[col].astype('category')
                    
                    changes.append({
                        "step": "Convert Data Type",
                        "column": col,
                        "target_type": target_type,
                        "rows_affected": len(cleaned_df)
                    })
                except Exception as e:
                    st.error(f"Error converting data type for {col}: {str(e)}")
        
        elif step_type == 'handle_outliers':
            col = step.get('column')
            method = step.get('method', 'cap')
            if col:
                try:
                    if method == 'cap':
                        # Calculate bounds
                        Q1 = cleaned_df[col].quantile(0.25)
                        Q3 = cleaned_df[col].quantile(0.75)
                        IQR = Q3 - Q1
                        lower_bound = Q1 - 1.5 * IQR
                        upper_bound = Q3 + 1.5 * IQR
                        
                        # Count outliers
                        outliers_count = ((cleaned_df[col] < lower_bound) | (cleaned_df[col] > upper_bound)).sum()
                        
                        # Cap outliers
                        cleaned_df[col] = cleaned_df[col].clip(lower=lower_bound, upper=upper_bound)
                        
                        changes.append({
                            "step": "Cap Outliers",
                            "column": col,
                            "lower_bound": float(lower_bound),
                            "upper_bound": float(upper_bound),
                            "rows_affected": int(outliers_count)
                        })
                except Exception as e:
                    st.error(f"Error handling outliers in {col}: {str(e)}")
        
        elif step_type == 'standardize_values':
            col = step.get('column')
            mapping = step.get('mapping', {})
            if col and mapping:
                try:
                    # Count affected rows
                    affected_rows = cleaned_df[cleaned_df[col].isin(mapping.keys())].shape[0]
                    
                    # Apply mapping
                    cleaned_df[col] = cleaned_df[col].replace(mapping)
                    
                    changes.append({
                        "step": "Standardize Values",
                        "column": col,
                        "mapping": mapping,
                        "rows_affected": int(affected_rows)
                    })
                except Exception as e:
                    st.error(f"Error standardizing values in {col}: {str(e)}")
    
    return cleaned_df, changes

@task(name="Save Cleaned Data", description="Save cleaned data to a new table in DuckDB", tags=["cleaning", "database"])
def save_cleaned_data(df, original_table_name):
    """Save cleaned data to a new table in DuckDB"""
    try:
        # Create a new table name with _cleaned suffix
        cleaned_table_name = f"{original_table_name}_cleaned"
        
        # Register the DataFrame with DuckDB
        con.register(cleaned_table_name, df)
        
        # Create a persistent table
        con.execute(f"CREATE OR REPLACE TABLE {cleaned_table_name} AS SELECT * FROM {cleaned_table_name}")
        
        return cleaned_table_name
    except Exception as e:
        st.error(f"Error saving cleaned data: {str(e)}")
        return None