"""
Prefect workflows for DataPlor data quality assessment and cleaning.

This module contains Prefect tasks and flows for automating data processing,
quality assessment, cleaning, and CPG-specific analysis.
"""

from prefect import task, flow
import pandas as pd
import duckdb
from typing import Dict, List, Tuple, Any

from util.cpg_analysis import (
    identify_chain_store_targets
)

# ----------------------
# Data Ingestion Tasks
# ----------------------

@task(name="Extract Data From Source", 
      description="Extract data from source file or API",
      tags=["data-ingestion"])
def extract_data_from_source(source_path: str) -> pd.DataFrame:
    """
    Extract data from a source file (CSV, Excel, etc.) or API.
    
    Args:
        source_path: Path to the source file or API endpoint
        
    Returns:
        DataFrame containing the extracted data
    """
    # Implementation would depend on the source type
    if source_path.endswith('.csv'):
        return pd.read_csv(source_path)
    elif source_path.endswith(('.xlsx', '.xls')):
        return pd.read_excel(source_path)
    else:
        raise ValueError(f"Unsupported source type: {source_path}")


@task(name="Transform Raw Data", 
      description="Apply initial transformations to raw data",
      tags=["data-ingestion"])
def transform_raw_data(data_frame: pd.DataFrame) -> pd.DataFrame:
    """
    Apply initial transformations to the raw data.
    
    Args:
        data_frame: Raw data frame
        
    Returns:
        Transformed data frame
    """
    # Apply basic transformations like column renaming, type conversion, etc.
    transformed_df = data_frame.copy()
    
    # Example transformations:
    # 1. Convert column names to lowercase
    transformed_df.columns = [col.lower() for col in transformed_df.columns]
    
    # 2. Remove leading/trailing whitespace from string columns
    for col in transformed_df.select_dtypes(include=['object']).columns:
        transformed_df[col] = transformed_df[col].str.strip()
    
    return transformed_df


@task(name="Load Data To DuckDB", 
      description="Load data into DuckDB table",
      tags=["data-ingestion"])
def load_data_to_duckdb(data_frame: pd.DataFrame, table_name: str) -> bool:
    """
    Load data into a DuckDB table.
    
    Args:
        data_frame: Data frame to load
        table_name: Name of the target table
        
    Returns:
        True if successful, False otherwise
    """
    try:
        conn = duckdb.connect('md:my_db')
        conn.register('temp_df', data_frame)
        conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_df")
        conn.close()
        return True
    except Exception as e:
        print(f"Error loading data to DuckDB: {str(e)}")
        return False


# ----------------------
# Data Quality Tasks
# ----------------------

@task(name="Assess Data Completeness", 
      description="Check for missing values and completeness issues",
      tags=["data-quality"])
def assess_data_completeness(table_name: str) -> Dict[str, Any]:
    """
    Assess the completeness of data in a table.
    
    Args:
        table_name: Name of the table to assess
        
    Returns:
        Dictionary with completeness metrics
    """
    conn = duckdb.connect('md:my_db')
    
    # Get column names
    columns = conn.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'").fetchall()
    columns = [col[0] for col in columns]
    
    # Calculate missing values for each column
    missing_counts = {}
    total_rows = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    
    for col in columns:
        missing_count = conn.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {col} IS NULL").fetchone()[0]
        missing_percentage = (missing_count / total_rows) * 100 if total_rows > 0 else 0
        missing_counts[col] = {
            "count": missing_count,
            "percentage": missing_percentage
        }
    
    conn.close()
    
    return {
        "total_rows": total_rows,
        "missing_values": missing_counts,
        "completeness_score": 100 - sum(item["percentage"] for item in missing_counts.values()) / len(columns)
    }


@task(name="Assess Data Consistency", 
      description="Check for data consistency issues",
      tags=["data-quality"])
def assess_data_consistency(table_name: str) -> Dict[str, Any]:
    """
    Assess the consistency of data in a table.
    
    Args:
        table_name: Name of the table to assess
        
    Returns:
        Dictionary with consistency metrics
    """
    conn = duckdb.connect('md:my_db')
    
    # Example: Check for inconsistent category values
    consistency_issues = {}
    
    # Check for category consistency if relevant columns exist
    try:
        category_counts = conn.execute(f"""
            SELECT 
                main_category, 
                COUNT(DISTINCT sub_category) as distinct_subcategories
            FROM {table_name}
            GROUP BY main_category
            ORDER BY distinct_subcategories DESC
        """).fetchall()
        
        consistency_issues["category_hierarchy"] = {
            "main_categories": len(category_counts),
            "potential_issues": [
                {"main_category": cat[0], "distinct_subcategories": cat[1]}
                for cat in category_counts if cat[1] > 10  # Arbitrary threshold
            ]
        }
    except:
        # Table might not have these columns
        pass
    
    conn.close()
    
    return {
        "consistency_issues": consistency_issues,
        "consistency_score": 85  # Placeholder - would be calculated based on actual issues
    }


@task(name="Assess Data Accuracy", 
      description="Check for data accuracy issues",
      tags=["data-quality"])
def assess_data_accuracy(table_name: str) -> Dict[str, Any]:
    """
    Assess the accuracy of data in a table.
    
    Args:
        table_name: Name of the table to assess
        
    Returns:
        Dictionary with accuracy metrics
    """
    conn = duckdb.connect('md:my_db')
    
    accuracy_issues = {}
    
    # Example: Check for outliers in numeric columns
    try:
        # Get numeric columns
        numeric_cols = conn.execute(f"""
            SELECT column_name FROM information_schema.columns 
            WHERE table_name = '{table_name}' 
            AND data_type IN ('INTEGER', 'BIGINT', 'DOUBLE', 'REAL')
        """).fetchall()
        
        numeric_cols = [col[0] for col in numeric_cols]
        
        outliers = {}
        for col in numeric_cols:
            # Calculate IQR and identify outliers
            stats = conn.execute(f"""
                SELECT 
                    percentile_cont(0.25) WITHIN GROUP (ORDER BY {col}) as q1,
                    percentile_cont(0.75) WITHIN GROUP (ORDER BY {col}) as q3
                FROM {table_name}
                WHERE {col} IS NOT NULL
            """).fetchone()
            
            if stats and stats[0] is not None and stats[1] is not None:
                q1, q3 = stats
                iqr = q3 - q1
                lower_bound = q1 - (1.5 * iqr)
                upper_bound = q3 + (1.5 * iqr)
                
                outlier_count = conn.execute(f"""
                    SELECT COUNT(*) FROM {table_name}
                    WHERE {col} < {lower_bound} OR {col} > {upper_bound}
                """).fetchone()[0]
                
                if outlier_count > 0:
                    outliers[col] = {
                        "count": outlier_count,
                        "lower_bound": lower_bound,
                        "upper_bound": upper_bound
                    }
        
        accuracy_issues["outliers"] = outliers
    except Exception as e:
        print(f"Error checking for outliers: {str(e)}")
    
    conn.close()
    
    return {
        "accuracy_issues": accuracy_issues,
        "accuracy_score": 90  # Placeholder - would be calculated based on actual issues
    }


@task(name="Generate Quality Report", 
      description="Generate a comprehensive data quality report",
      tags=["data-quality"])
def generate_quality_report(metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate a comprehensive data quality report.
    
    Args:
        metrics: Dictionary with various quality metrics
        
    Returns:
        Dictionary with the quality report
    """
    # Calculate overall quality score
    completeness_score = metrics.get("completeness", {}).get("completeness_score", 0)
    consistency_score = metrics.get("consistency", {}).get("consistency_score", 0)
    accuracy_score = metrics.get("accuracy", {}).get("accuracy_score", 0)
    
    overall_score = (completeness_score + consistency_score + accuracy_score) / 3
    
    # Determine if cleaning is needed
    needs_cleaning = overall_score < 85 or completeness_score < 80
    
    return {
        "overall_quality_score": overall_score,
        "dimension_scores": {
            "completeness": completeness_score,
            "consistency": consistency_score,
            "accuracy": accuracy_score
        },
        "needs_cleaning": needs_cleaning,
        "detailed_metrics": metrics,
        "timestamp": pd.Timestamp.now().isoformat()
    }


# ----------------------
# Data Cleaning Tasks
# ----------------------

@task(name="Identify Cleaning Operations", 
      description="Identify what cleaning operations are needed",
      tags=["data-cleaning"])
def identify_cleaning_operations(table_name: str, quality_report: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Identify what cleaning operations are needed based on the quality report.
    
    Args:
        table_name: Name of the table to clean
        quality_report: Data quality report
        
    Returns:
        List of cleaning operations to perform
    """
    cleaning_operations = []
    
    # Check for missing values
    missing_values = quality_report.get("detailed_metrics", {}).get("completeness", {}).get("missing_values", {})
    for col, info in missing_values.items():
        if info["percentage"] > 5:  # Arbitrary threshold
            cleaning_operations.append({
                "type": "missing_values",
                "column": col,
                "strategy": "impute",  # Could be 'drop', 'impute', etc.
                "details": info
            })
    
    # Check for outliers
    outliers = quality_report.get("detailed_metrics", {}).get("accuracy", {}).get("accuracy_issues", {}).get("outliers", {})
    for col, info in outliers.items():
        if info["count"] > 10:  # Arbitrary threshold
            cleaning_operations.append({
                "type": "outliers",
                "column": col,
                "strategy": "clip",  # Could be 'clip', 'remove', etc.
                "details": info
            })
    
    # Check for duplicates
    # This would require additional analysis
    conn = duckdb.connect('md:my_db')
    try:
        # Simple check for exact duplicates
        duplicate_count = conn.execute(f"""
            SELECT COUNT(*) - COUNT(DISTINCT *) FROM {table_name}
        """).fetchone()[0]
        
        if duplicate_count > 0:
            cleaning_operations.append({
                "type": "duplicates",
                "strategy": "remove_exact",
                "details": {"count": duplicate_count}
            })
    except Exception as e:
        print(f"Error checking for duplicates: {str(e)}")
    
    conn.close()
    
    return cleaning_operations


@task(name="Apply Cleaning Operations", 
      description="Apply the cleaning operations to the data",
      tags=["data-cleaning"])
def apply_cleaning_operations(table_name: str, operations: List[Dict[str, Any]]) -> str:
    """
    Apply the cleaning operations to the data.
    
    Args:
        table_name: Name of the table to clean
        operations: List of cleaning operations to perform
        
    Returns:
        Name of the cleaned table
    """
    conn = duckdb.connect('md:my_db')
    
    # Create a new table for the cleaned data
    cleaned_table_name = f"{table_name}_cleaned"
    
    # Start with a copy of the original table
    conn.execute(f"CREATE OR REPLACE TABLE {cleaned_table_name} AS SELECT * FROM {table_name}")
    
    # Apply each cleaning operation
    for op in operations:
        if op["type"] == "missing_values" and op["strategy"] == "impute":
            column = op["column"]
            
            # Determine imputation method based on data type
            col_type = conn.execute(f"""
                SELECT data_type FROM information_schema.columns 
                WHERE table_name = '{cleaned_table_name}' AND column_name = '{column}'
            """).fetchone()[0]
            
            if col_type in ('INTEGER', 'BIGINT', 'DOUBLE', 'REAL'):
                # For numeric columns, use median
                conn.execute(f"""
                    UPDATE {cleaned_table_name}
                    SET {column} = (
                        SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY {column})
                        FROM {cleaned_table_name}
                        WHERE {column} IS NOT NULL
                    )
                    WHERE {column} IS NULL
                """)
            else:
                # For categorical columns, use mode
                conn.execute(f"""
                    UPDATE {cleaned_table_name}
                    SET {column} = (
                        SELECT {column}
                        FROM {cleaned_table_name}
                        WHERE {column} IS NOT NULL
                        GROUP BY {column}
                        ORDER BY COUNT(*) DESC
                        LIMIT 1
                    )
                    WHERE {column} IS NULL
                """)
        
        elif op["type"] == "outliers" and op["strategy"] == "clip":
            column = op["column"]
            lower_bound = op["details"]["lower_bound"]
            upper_bound = op["details"]["upper_bound"]
            
            conn.execute(f"""
                UPDATE {cleaned_table_name}
                SET {column} = CASE
                    WHEN {column} < {lower_bound} THEN {lower_bound}
                    WHEN {column} > {upper_bound} THEN {upper_bound}
                    ELSE {column}
                END
            """)
        
        elif op["type"] == "duplicates" and op["strategy"] == "remove_exact":
            # Create a temporary table with distinct rows
            conn.execute(f"""
                CREATE OR REPLACE TABLE temp_distinct AS
                SELECT DISTINCT * FROM {cleaned_table_name}
            """)
            
            # Replace the cleaned table with the distinct rows
            conn.execute(f"""
                DROP TABLE {cleaned_table_name};
                ALTER TABLE temp_distinct RENAME TO {cleaned_table_name};
            """)
    
    conn.close()
    
    return cleaned_table_name


@task(name="Validate Cleaned Data", 
      description="Validate that the cleaned data meets quality standards",
      tags=["data-cleaning"])
def validate_cleaned_data(cleaned_table_name: str) -> Dict[str, Any]:
    """
    Validate that the cleaned data meets quality standards.
    
    Args:
        cleaned_table_name: Name of the cleaned table
        
    Returns:
        Dictionary with validation results
    """
    # Run the quality assessment tasks on the cleaned data
    completeness = assess_data_completeness(cleaned_table_name)
    consistency = assess_data_consistency(cleaned_table_name)
    accuracy = assess_data_accuracy(cleaned_table_name)
    
    # Generate a quality report for the cleaned data
    cleaned_quality_report = generate_quality_report({
        "completeness": completeness,
        "consistency": consistency,
        "accuracy": accuracy
    })
    
    # Determine if the cleaning was successful
    original_score = 0  # This would come from the original quality report
    cleaned_score = cleaned_quality_report["overall_quality_score"]
    improvement = cleaned_score - original_score
    
    return {
        "passed": cleaned_score >= 85,  # Arbitrary threshold
        "original_score": original_score,
        "cleaned_score": cleaned_score,
        "improvement": improvement,
        "quality_report": cleaned_quality_report
    }


# ----------------------
# CPG Analysis Tasks
# ----------------------

@task(name="Analyze Chain Store Data", 
      description="Analyze chain store data for CPG insights",
      tags=["cpg-analysis"])
def analyze_chain_store_data(table_name: str, min_locations: int = 3) -> Dict[str, Any]:
    """
    Analyze chain store data for CPG insights.
    
    Args:
        table_name: Name of the table to analyze
        min_locations: Minimum number of locations for a chain
        
    Returns:
        Dictionary with chain store analysis results
    """
    conn = duckdb.connect('md:my_db')
    
    # Use the existing CPG analysis function
    chains_df = identify_chain_store_targets(conn, table_name, min_locations)
    
    # Additional analysis
    total_chains = len(chains_df)
    total_chain_locations = chains_df['location_count'].sum() if not chains_df.empty else 0
    
    # Calculate percentage of locations that are part of chains
    total_locations = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    chain_percentage = (total_chain_locations / total_locations) * 100 if total_locations > 0 else 0
    
    conn.close()
    
    return {
        "total_chains": total_chains,
        "total_chain_locations": total_chain_locations,
        "chain_percentage": chain_percentage,
        "chains_data": chains_df.to_dict('records') if not chains_df.empty else []
    }


@task(name="Analyze Geographic Coverage", 
      description="Analyze geographic coverage for CPG distribution",
      tags=["cpg-analysis"])
def analyze_geographic_coverage_data(table_name: str) -> Dict[str, Any]:
    """
    Analyze geographic coverage for CPG distribution.
    
    Args:
        table_name: Name of the table to analyze
        
    Returns:
        Dictionary with geographic coverage analysis results
    """
    conn = duckdb.connect('md:my_db')
    
    # Check if the table has geographic columns
    has_geo_columns = True
    try:
        conn.execute(f"SELECT state, city FROM {table_name} LIMIT 1")
    except Exception as e:
        has_geo_columns = False
        print(f"Error checking geographic columns: {str(e)}")
    
    if not has_geo_columns:
        conn.close()
        return {
            "error": "Table does not have required geographic columns"
        }
    
    # Analyze state coverage
    state_coverage = conn.execute(f"""
        SELECT 
            state, 
            COUNT(*) as location_count,
            COUNT(DISTINCT city) as city_count
        FROM {table_name}
        WHERE state IS NOT NULL
        GROUP BY state
        ORDER BY location_count DESC
    """).fetchall()
    
    # Analyze city concentration
    city_concentration = conn.execute(f"""
        SELECT 
            state,
            city,
            COUNT(*) as location_count
        FROM {table_name}
        WHERE state IS NOT NULL AND city IS NOT NULL
        GROUP BY state, city
        ORDER BY location_count DESC
        LIMIT 20
    """).fetchall()
    
    conn.close()
    
    return {
        "state_coverage": [
            {"state": s[0], "location_count": s[1], "city_count": s[2]}
            for s in state_coverage
        ],
        "city_concentration": [
            {"state": c[0], "city": c[1], "location_count": c[2]}
            for c in city_concentration
        ],
        "total_states": len(state_coverage),
        "total_cities": sum(s[2] for s in state_coverage)
    }


@task(name="Analyze Category Consistency", 
      description="Analyze category consistency for CPG products",
      tags=["cpg-analysis"])
def analyze_category_consistency(table_name: str) -> Dict[str, Any]:
    """
    Analyze category consistency for CPG products.
    
    Args:
        table_name: Name of the table to analyze
        
    Returns:
        Dictionary with category consistency analysis results
    """
    conn = duckdb.connect('md:my_db')
    
    # Check if the table has category columns
    has_category_columns = True
    try:
        conn.execute(f"SELECT main_category, sub_category FROM {table_name} LIMIT 1")
    except Exception as e:
        has_category_columns = False
        print(f"Error checking category columns: {str(e)}")
    
    if not has_category_columns:
        conn.close()
        return {
            "error": "Table does not have required category columns"
        }
    
    # Analyze main categories
    main_categories = conn.execute(f"""
        SELECT 
            main_category, 
            COUNT(*) as location_count,
            COUNT(DISTINCT sub_category) as sub_category_count
        FROM {table_name}
        WHERE main_category IS NOT NULL
        GROUP BY main_category
        ORDER BY location_count DESC
    """).fetchall()
    
    # Analyze unusual category combinations
    unusual_combinations = conn.execute(f"""
        WITH category_counts AS (
            SELECT 
                main_category, 
                sub_category,
                COUNT(*) as count
            FROM {table_name}
            WHERE main_category IS NOT NULL AND sub_category IS NOT NULL
            GROUP BY main_category, sub_category
        )
        SELECT 
            main_category,
            sub_category,
            count,
            count * 100.0 / SUM(count) OVER (PARTITION BY main_category) as percentage
        FROM category_counts
        WHERE count <= 5
        ORDER BY main_category, count DESC
    """).fetchall()
    
    conn.close()
    
    return {
        "main_categories": [
            {"main_category": m[0], "location_count": m[1], "sub_category_count": m[2]}
            for m in main_categories
        ],
        "unusual_combinations": [
            {"main_category": u[0], "sub_category": u[1], "count": u[2], "percentage": u[3]}
            for u in unusual_combinations
        ],
        "total_main_categories": len(main_categories),
        "total_unusual_combinations": len(unusual_combinations)
    }


# ----------------------
# Prefect Flows
# ----------------------

@flow(name="Data Ingestion Pipeline",
      description="Extract, transform, and load data into DuckDB")
def data_ingestion_pipeline(source_path: str, table_name: str) -> bool:
    """
    Extract, transform, and load data into DuckDB.
    
    Args:
        source_path: Path to the source file or API endpoint
        table_name: Name of the target table
        
    Returns:
        True if successful, False otherwise
    """
    raw_data = extract_data_from_source(source_path)
    transformed_data = transform_raw_data(raw_data)
    result = load_data_to_duckdb(transformed_data, table_name)
    return result


@flow(name="Data Quality Assessment Pipeline",
      description="Assess data quality across multiple dimensions")
def data_quality_assessment_pipeline(table_name: str) -> Dict[str, Any]:
    """
    Assess data quality across multiple dimensions.
    
    Args:
        table_name: Name of the table to assess
        
    Returns:
        Dictionary with quality assessment results
    """
    completeness = assess_data_completeness(table_name)
    consistency = assess_data_consistency(table_name)
    accuracy = assess_data_accuracy(table_name)
    
    metrics = {
        "completeness": completeness,
        "consistency": consistency,
        "accuracy": accuracy
    }
    
    report = generate_quality_report(metrics)
    return report


@flow(name="Data Cleaning Pipeline",
      description="Clean data based on quality assessment")
def data_cleaning_pipeline(table_name: str, quality_report: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    """
    Clean data based on quality assessment.
    
    Args:
        table_name: Name of the table to clean
        quality_report: Data quality report
        
    Returns:
        Tuple of (cleaned_table_name, validation_results)
    """
    operations = identify_cleaning_operations(table_name, quality_report)
    cleaned_table = apply_cleaning_operations(table_name, operations)
    validation = validate_cleaned_data(cleaned_table)
    return cleaned_table, validation


@flow(name="CPG Analysis Pipeline",
      description="Run CPG-specific analyses")
def cpg_analysis_pipeline(table_name: str) -> Dict[str, Any]:
    """
    Run CPG-specific analyses.
    
    Args:
        table_name: Name of the table to analyze
        
    Returns:
        Dictionary with CPG analysis results
    """
    chain_results = analyze_chain_store_data(table_name)
    coverage_results = analyze_geographic_coverage_data(table_name)
    category_results = analyze_category_consistency(table_name)
    
    return {
        "chain_analysis": chain_results,
        "coverage_analysis": coverage_results,
        "category_analysis": category_results
    }


@flow(name="Master Data Quality Flow",
      description="End-to-end data quality assessment and improvement")
def master_data_quality_flow(source_path: str, table_name: str) -> Dict[str, Any]:
    """
    Run the entire pipeline end-to-end.
    
    Args:
        source_path: Path to the source file or API endpoint
        table_name: Name of the target table
        
    Returns:
        Dictionary with pipeline results
    """
    # Step 1: Data Ingestion
    ingestion_result = data_ingestion_pipeline(source_path, table_name)
    
    if not ingestion_result:
        return {"error": "Data ingestion failed"}
    
    # Step 2: Data Quality Assessment
    quality_report = data_quality_assessment_pipeline(table_name)
    
    # Step 3: Data Cleaning (if needed)
    if quality_report["needs_cleaning"]:
        cleaned_table, validation = data_cleaning_pipeline(table_name, quality_report)
        
        # Step 4: CPG Analysis (on cleaned data if cleaning was successful)
        if validation["passed"]:
            analysis_results = cpg_analysis_pipeline(cleaned_table)
            
            return {
                "quality_report": quality_report,
                "cleaning_validation": validation,
                "analysis_results": analysis_results,
                "cleaned_table": cleaned_table
            }
        else:
            # Run analysis on original data if cleaning didn't improve quality
            analysis_results = cpg_analysis_pipeline(table_name)
            
            return {
                "quality_report": quality_report,
                "cleaning_validation": validation,
                "analysis_results": analysis_results,
                "warning": "Analysis performed on original data as cleaning did not meet quality standards"
            }
    else:
        # No cleaning needed, run analysis on original data
        analysis_results = cpg_analysis_pipeline(table_name)
        
        return {
            "quality_report": quality_report,
            "analysis_results": analysis_results
        }
