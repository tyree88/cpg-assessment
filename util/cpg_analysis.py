#!/usr/bin/env python3
"""
CPG Data Analysis Module

This module provides functions to analyze CPG (Consumer Packaged Goods) data in a DuckDB database,
focusing on retail distribution, market analysis, territory management, and data quality.

Usage:
    import util.cpg_analysis as cpg_analysis
    
    # Connect to DuckDB
    conn = cpg_analysis.connect_db('my_db')
    
    # Run analyses
    distribution_points = cpg_analysis.get_active_distribution_points(conn)
    chains = cpg_analysis.identify_chain_store_targets(conn)
    
    # Get all analyses as a dictionary
    results = cpg_analysis.run_all_analyses(conn)
"""

import duckdb
import pandas as pd
from typing import Dict, Tuple, Optional
from prefect import task, flow

from util.sql_queries import (
    get_chain_store_targets_query,
    get_active_distribution_points_query,
    get_distribution_gaps_query,
    get_retail_segments_query,
    get_territory_coverage_query,
    get_data_completeness_query
)
from util.database import get_db_connection


@task(name="Connect to Database", description="Connect to DuckDB database and verify table exists")
def connect_db(db_path: str, table_name: str = 'boisedemodatasampleaug') -> Tuple[duckdb.DuckDBPyConnection, str]:
    """
    Connect to DuckDB database and verify table exists.
    
    Args:
        db_path: Path to the DuckDB database
        table_name: Name of the table containing location data
        
    Returns:
        Tuple of database connection and table name
    """
    conn = duckdb.connect(db_path)
    
    # Verify table exists
    try:
        conn.execute(f"SELECT * FROM {table_name} LIMIT 1")
    except duckdb.CatalogException:
        raise ValueError(f"Table '{table_name}' not found in database {db_path}")
    
    return conn, table_name


# ===============================================
# 1. STORE/RETAILER TARGETING & DISTRIBUTION PLANNING
# ===============================================

@task(name="Get Active Distribution Points", description="Identify active retail and grocery locations for distribution planning", tags=["cpg-analysis"])
def get_active_distribution_points(
    conn: duckdb.DuckDBPyConnection, 
    table_name: str = 'boisedemodatasampleaug',
    min_confidence: float = 0.0, 
    city: Optional[str] = None
) -> pd.DataFrame:
    """
    Identify active retail and grocery locations for distribution planning.
    
    Args:
        conn: DuckDB database connection
        table_name: Name of the table containing location data
        min_confidence: Minimum data quality confidence score (0.0-1.0)
        city: Filter results to a specific city
        
    Returns:
        DataFrame with active retail/grocery locations
    """
    query = get_active_distribution_points_query(table_name, min_confidence, city)
    return conn.execute(query).df()


@task(name="Analyze Delivery Windows", description="Analyze delivery windows based on business hours", tags=["cpg-analysis"])
def analyze_delivery_windows(
    conn: duckdb.DuckDBPyConnection, 
    table_name: str = 'boisedemodatasampleaug',
    day_of_week: str = 'monday'
) -> pd.DataFrame:
    """
    Analyze delivery windows based on business hours.
    
    Args:
        conn: DuckDB database connection
        table_name: Name of the table containing location data
        day_of_week: Day of the week to analyze (monday, tuesday, etc.)
        
    Returns:
        DataFrame with delivery window information
    """
    day_of_week = day_of_week.lower()
    valid_days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    
    if day_of_week not in valid_days:
        raise ValueError(f"Invalid day. Must be one of: {', '.join(valid_days)}")
    
    query = f"""
    SELECT 
        dataplor_id,
        name,
        address,
        city,
        main_category,
        sub_category,
        CASE 
            WHEN {day_of_week}_open IS NOT NULL THEN {day_of_week}_open
            ELSE '09:00:00' -- Default assumption if missing
        END AS {day_of_week}_open,
        CASE 
            WHEN {day_of_week}_close IS NOT NULL THEN {day_of_week}_close
            ELSE '17:00:00' -- Default assumption if missing
        END AS {day_of_week}_close,
        -- Calculate estimated delivery window in hours
        CASE 
            WHEN {day_of_week}_open IS NOT NULL AND {day_of_week}_close IS NOT NULL 
            THEN EXTRACT(HOUR FROM {day_of_week}_close) - EXTRACT(HOUR FROM {day_of_week}_open)
            ELSE 8 -- Default assumption if missing
        END AS window_hours
    FROM {table_name}
    WHERE main_category IN ('retail', 'convenience_and_grocery_stores')
        AND open_closed_status = 'open'
    ORDER BY window_hours DESC
    """
    
    return conn.execute(query).df()


@task(name="Identify Chain Store Targets", description="Identify chain stores with multiple locations for efficient distribution deals", tags=["cpg-analysis"])
def identify_chain_store_targets(
    conn: duckdb.DuckDBPyConnection, 
    table_name: str = 'boisedemodatasampleaug',
    min_locations: int = 2
) -> pd.DataFrame:
    """
    Identify chain stores with multiple locations for efficient distribution deals.
    
    Args:
        conn: DuckDB database connection
        table_name: Name of the table containing location data
        min_locations: Minimum number of locations a chain must have
        
    Returns:
        DataFrame with chain store information
    """
    query = get_chain_store_targets_query(table_name, min_locations)
    return conn.execute(query).df()


@task(name="Find Distribution Gaps", description="Identify cities with limited retail coverage", tags=["cpg-analysis"])
def find_distribution_gaps(
    conn: duckdb.DuckDBPyConnection, 
    table_name: str = 'boisedemodatasampleaug',
    min_locations: int = 20
) -> pd.DataFrame:
    """
    Identify cities with limited retail coverage.
    
    Args:
        conn: DuckDB database connection
        table_name: Name of the table containing location data
        min_locations: Minimum number of total locations for a city to be included
        
    Returns:
        DataFrame with retail distribution gaps by city
    """
    query = get_distribution_gaps_query(table_name, min_locations)
    
    return conn.execute(query).df()


# ===============================================
# 2. MARKET ANALYSIS & COMPETITIVE INTELLIGENCE
# ===============================================

@task(name="Analyze Retail Segments", description="Analyze the distribution of retail categories", tags=["cpg-analysis"])
def analyze_retail_segments(
    conn: duckdb.DuckDBPyConnection, 
    table_name: str = 'boisedemodatasampleaug'
) -> pd.DataFrame:
    """
    Analyze the distribution of retail categories.
    
    Args:
        conn: DuckDB database connection
        table_name: Name of the table containing location data
        
    Returns:
        DataFrame with retail segment analysis
    """
    query = get_retail_segments_query(table_name)
    
    return conn.execute(query).df()


@task(name="Analyze Competitive Density", description="Map retail density by postal code for competitive intelligence", tags=["cpg-analysis"])
def analyze_competitive_density(
    conn: duckdb.DuckDBPyConnection, 
    table_name: str = 'boisedemodatasampleaug',
    top_n: int = 10
) -> pd.DataFrame:
    """
    Map retail density by postal code for competitive intelligence.
    
    Args:
        conn: DuckDB database connection
        table_name: Name of the table containing location data
        top_n: Number of top postal codes to return
        
    Returns:
        DataFrame with competitive density by postal code
    """
    query = f"""
    SELECT 
        postal_code,
        city,
        COUNT(*) AS total_locations,
        SUM(CASE WHEN main_category = 'retail' THEN 1 ELSE 0 END) AS retail_locations,
        SUM(CASE WHEN main_category = 'convenience_and_grocery_stores' THEN 1 ELSE 0 END) AS grocery_locations,
        SUM(CASE WHEN chain_id IS NOT NULL AND chain_id != '' THEN 1 ELSE 0 END) AS chain_locations,
        STRING_AGG(DISTINCT sub_category, ', ') AS retail_types
    FROM {table_name}
    WHERE open_closed_status = 'open'
        AND postal_code IS NOT NULL
        AND main_category IN ('retail', 'convenience_and_grocery_stores')
    GROUP BY postal_code, city
    ORDER BY total_locations DESC
    LIMIT {top_n}
    """
    
    return conn.execute(query).df()


@task(name="Compare Customer Engagement", description="Compare customer engagement metrics across retail categories", tags=["cpg-analysis"])
def compare_customer_engagement(
    conn: duckdb.DuckDBPyConnection, 
    table_name: str = 'boisedemodatasampleaug',
    min_locations: int = 5
) -> pd.DataFrame:
    """
    Compare customer engagement metrics across retail categories.
    
    Args:
        conn: DuckDB database connection
        table_name: Name of the table containing location data
        min_locations: Minimum number of locations for a category to be included
        
    Returns:
        DataFrame with customer engagement comparison
    """
    query = f"""
    SELECT 
        main_category,
        sub_category,
        COUNT(*) AS location_count,
        ROUND(AVG(CAST(popularity_score AS FLOAT)), 1) AS avg_popularity,
        ROUND(AVG(CAST(sentiment_score AS FLOAT)), 2) AS avg_sentiment,
        ROUND(AVG(CAST(dwell_time AS FLOAT)), 1) AS avg_dwell_minutes
    FROM {table_name}
    WHERE main_category IN ('retail', 'convenience_and_grocery_stores', 'dining')
        AND open_closed_status = 'open'
    GROUP BY main_category, sub_category
    HAVING COUNT(*) > {min_locations}
    ORDER BY avg_popularity DESC, avg_sentiment DESC
    """
    
    return conn.execute(query).df()


# ===============================================
# 3. TERRITORY MANAGEMENT
# ===============================================

@task(name="Analyze Territory Coverage", description="Map retail distribution by city for sales territory planning", tags=["cpg-analysis"])
def analyze_territory_coverage(
    conn: duckdb.DuckDBPyConnection, 
    table_name: str = 'boisedemodatasampleaug'
) -> pd.DataFrame:
    """
    Map retail distribution by city for sales territory planning.
    
    Args:
        conn: DuckDB database connection
        table_name: Name of the table containing location data
        
    Returns:
        DataFrame with territory coverage analysis
    """
    query = get_territory_coverage_query(table_name)
    
    return conn.execute(query).df()


@task(name="Analyze Geographic Clusters", description="Identify retail clusters for efficient territory visits", tags=["cpg-analysis"])
def analyze_geographic_clusters(
    conn: duckdb.DuckDBPyConnection, 
    table_name: str = 'boisedemodatasampleaug',
    min_cluster_size: int = 3
) -> pd.DataFrame:
    """
    Identify retail clusters for efficient territory visits.
    
    Args:
        conn: DuckDB database connection
        table_name: Name of the table containing location data
        min_cluster_size: Minimum number of locations in a cluster
        
    Returns:
        DataFrame with geographic cluster analysis
    """
    query = f"""
    SELECT 
        postal_code,
        ROUND(latitude, 2) AS latitude_area,
        ROUND(longitude, 2) AS longitude_area,
        COUNT(*) AS locations_in_cluster,
        STRING_AGG(DISTINCT main_category, ', ') AS business_types,
        STRING_AGG(DISTINCT chain_name, ', ') AS chains_in_area
    FROM {table_name}
    WHERE main_category IN ('retail', 'convenience_and_grocery_stores')
        AND open_closed_status = 'open'
        AND latitude IS NOT NULL
        AND longitude IS NOT NULL
    GROUP BY postal_code, ROUND(latitude, 2), ROUND(longitude, 2)
    HAVING COUNT(*) >= {min_cluster_size}
    ORDER BY locations_in_cluster DESC
    """
    
    return conn.execute(query).df()


# ===============================================
# 4. DATA QUALITY ASSESSMENT
# ===============================================

@task(name="Assess Critical Data Completeness", description="Assess completeness of critical fields for CPG operations", tags=["cpg-analysis", "data-quality"])
def assess_critical_data_completeness(
    conn: duckdb.DuckDBPyConnection, 
    table_name: str = 'boisedemodatasampleaug'
) -> pd.DataFrame:
    """
    Assess completeness of critical fields for CPG operations.
    
    Args:
        conn: DuckDB database connection
        table_name: Name of the table containing location data
        
    Returns:
        DataFrame with data completeness assessment
    """
    query = get_data_completeness_query(table_name)
    
    return conn.execute(query).df()


@task(name="Assess Chain Data Quality", description="Assess data quality for important CPG retail chains", tags=["cpg-analysis", "data-quality"])
def assess_chain_data_quality(
    conn: duckdb.DuckDBPyConnection, 
    table_name: str = 'boisedemodatasampleaug',
    min_locations: int = 3
) -> pd.DataFrame:
    """
    Assess data quality for important CPG retail chains.
    
    Args:
        conn: DuckDB database connection
        table_name: Name of the table containing location data
        min_locations: Minimum number of locations a chain must have
        
    Returns:
        DataFrame with chain data quality assessment
    """
    query = f"""
    SELECT 
        chain_name,
        COUNT(*) AS total_locations,
        SUM(CASE WHEN address IS NULL THEN 1 ELSE 0 END) AS missing_address,
        SUM(CASE WHEN monday_open IS NULL OR monday_close IS NULL THEN 1 ELSE 0 END) AS missing_hours,
        SUM(CASE WHEN website IS NULL THEN 1 ELSE 0 END) AS missing_website,
        ROUND(AVG(data_quality_confidence_score), 2) AS avg_confidence_score
    FROM {table_name}
    WHERE main_category IN ('retail', 'convenience_and_grocery_stores')
        AND open_closed_status = 'open'
        AND chain_id IS NOT NULL
        AND chain_id != ''
    GROUP BY chain_name
    HAVING COUNT(*) > {min_locations}
    ORDER BY total_locations DESC, avg_confidence_score
    """
    
    return conn.execute(query).df()


@flow(name="CPG Analysis Flow", description="Run all CPG data analyses and return results")
def run_all_analyses(
    db_path: str = 'md:my_db', 
    table_name: str = 'boisedemodatasampleaug'
) -> Dict[str, pd.DataFrame]:
    """
    Run all CPG data analyses and return results in a dictionary.
    
    Args:
        db_path: Path to the DuckDB database
        table_name: Name of the table containing location data
        
    Returns:
        Dictionary of analysis results keyed by analysis name
    """
    results = {}
    
    with get_db_connection(db_path) as conn:
        # Distribution Planning
        results['distribution_points'] = get_active_distribution_points(conn, table_name)
        results['delivery_windows'] = analyze_delivery_windows(conn, table_name)
        results['chain_targets'] = identify_chain_store_targets(conn, table_name)
        results['distribution_gaps'] = find_distribution_gaps(conn, table_name)
        
        # Market Analysis
        results['retail_segments'] = analyze_retail_segments(conn, table_name)
        results['competitive_density'] = analyze_competitive_density(conn, table_name)
        results['customer_engagement'] = compare_customer_engagement(conn, table_name)
        
        # Territory Management
        results['territory_coverage'] = analyze_territory_coverage(conn, table_name)
        results['geographic_clusters'] = analyze_geographic_clusters(conn, table_name)
        
        # Data Quality
        results['data_completeness'] = assess_critical_data_completeness(conn, table_name)
        results['chain_data_quality'] = assess_chain_data_quality(conn, table_name)
    
    return results


if __name__ == "__main__":
    import argparse
    import os
    
    parser = argparse.ArgumentParser(description="Analyze CPG data in DuckDB")
    parser.add_argument('--db', type=str, default='my_db', help='Path to DuckDB database')
    parser.add_argument('--table', type=str, default='boisedemodatasampleaug', help='Table name')
    parser.add_argument('--output', type=str, help='Output directory for results (optional)')
    parser.add_argument('--analysis', type=str, choices=[
        'distribution_points', 'delivery_windows', 'chain_targets', 'distribution_gaps',
        'retail_segments', 'competitive_density', 'customer_engagement',
        'territory_coverage', 'geographic_clusters',
        'data_completeness', 'chain_data_quality',
        'all'
    ], default='all', help='Specific analysis to run (default: all)')
    
    args = parser.parse_args()
    
    try:
        # Connect to database
        conn, table_name = connect_db(args.db, args.table)
        print(f"Connected to database: {args.db}, table: {table_name}")
        
        # Run analysis
        if args.analysis == 'all':
            print("Running all analyses...")
            results = run_all_analyses(conn, table_name)
            
            # Print summary
            for name, df in results.items():
                print(f"\n{name}: {len(df)} rows")
                print(df.head(3))
                
            # Save results if output directory provided
            if args.output:
                os.makedirs(args.output, exist_ok=True)
                for name, df in results.items():
                    output_path = os.path.join(args.output, f"{name}.csv")
                    df.to_csv(output_path, index=False)
                print(f"\nResults saved to {args.output}")
        else:
            # Run specific analysis
            print(f"Running analysis: {args.analysis}")
            analysis_func = globals()[args.analysis]
            result = analysis_func(conn, table_name)
            
            print(f"\n{args.analysis}: {len(result)} rows")
            print(result.head(10))
            
            # Save result if output directory provided
            if args.output:
                os.makedirs(args.output, exist_ok=True)
                output_path = os.path.join(args.output, f"{args.analysis}.csv")
                result.to_csv(output_path, index=False)
                print(f"\nResult saved to {output_path}")
                
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
