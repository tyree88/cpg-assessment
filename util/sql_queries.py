#!/usr/bin/env python3
"""
SQL Query Module

Provides optimized SQL queries for the dataplor application.
"""

from typing import Optional

# Common filter conditions
RETAIL_FILTER = "main_category IN ('retail', 'convenience_and_grocery_stores')"
OPEN_FILTER = "open_closed_status = 'open'"
VALID_CHAIN_FILTER = "chain_id IS NOT NULL AND chain_id != ''"

def create_materialized_view(conn, view_name: str, table_name: str) -> None:
    """Create a materialized view for frequently accessed retail data."""
    query = f"""
    CREATE OR REPLACE VIEW {view_name} AS
    SELECT *
    FROM {table_name}
    WHERE {RETAIL_FILTER}
      AND {OPEN_FILTER}
    """
    conn.execute(query)

def get_chain_store_targets_query(table_name: str, min_locations: int = 2) -> str:
    """Get optimized query for chain store targets."""
    return f"""
    SELECT 
        chain_name,
        COUNT(*) AS location_count,
        STRING_AGG(DISTINCT city, ', ') AS cities,
        MIN(data_quality_confidence_score) AS min_confidence,
        MAX(data_quality_confidence_score) AS max_confidence,
        AVG(data_quality_confidence_score) AS avg_confidence
    FROM {table_name}
    WHERE {RETAIL_FILTER}
        AND {OPEN_FILTER}
        AND {VALID_CHAIN_FILTER}
    GROUP BY chain_name
    HAVING COUNT(*) >= {min_locations}
    ORDER BY location_count DESC
    """

def get_active_distribution_points_query(
    table_name: str,
    min_confidence: float = 0.0, 
    city: Optional[str] = None
) -> str:
    """Get optimized query for active distribution points."""
    query = f"""
    SELECT 
        dataplor_id,
        name,
        chain_name,
        main_category,
        sub_category,
        address,
        city,
        state,
        postal_code,
        latitude,
        longitude,
        open_closed_status,
        data_quality_confidence_score
    FROM {table_name}
    WHERE {RETAIL_FILTER}
        AND {OPEN_FILTER}
        AND address IS NOT NULL
        AND data_quality_confidence_score >= {min_confidence}
    """
    
    if city and city != 'All Cities':
        query += f" AND city = '{city}'"
        
    query += " ORDER BY city, data_quality_confidence_score DESC"
    
    return query

def get_distribution_gaps_query(table_name: str, min_locations: int = 20) -> str:
    """Get optimized query for distribution gaps."""
    return f"""
    -- Use a CTE to filter the base data once
    WITH retail_locations AS (
        SELECT 
            city,
            state,
            sub_category
        FROM {table_name}
        WHERE {RETAIL_FILTER}
            AND {OPEN_FILTER}
            AND city IS NOT NULL
            AND state IS NOT NULL
    ),
    -- Calculate metrics per city
    city_metrics AS (
        SELECT 
            city,
            state,
            COUNT(*) AS location_count,
            COUNT(DISTINCT sub_category) AS category_count
        FROM retail_locations
        GROUP BY city, state
    )
    -- Final selection with gap identification
    SELECT 
        city,
        state,
        location_count,
        category_count
    FROM city_metrics
    WHERE location_count < {min_locations}
    ORDER BY location_count ASC
    """

def get_retail_segments_query(table_name: str) -> str:
    """Get optimized query for retail segments analysis."""
    return f"""
    WITH retail_categories AS (
        SELECT 
            sub_category AS category,
            COUNT(*) AS location_count
        FROM {table_name}
        WHERE {RETAIL_FILTER}
            AND {OPEN_FILTER}
            AND sub_category IS NOT NULL
            AND sub_category != ''
        GROUP BY sub_category
    )
    SELECT 
        category,
        location_count,
        ROUND(location_count * 100.0 / (SELECT SUM(location_count) FROM retail_categories), 2) AS percentage
    FROM retail_categories
    ORDER BY location_count DESC
    """

def get_territory_coverage_query(table_name: str) -> str:
    """Get optimized query for territory coverage analysis."""
    return f"""
    WITH city_coverage AS (
        SELECT 
            city,
            state,
            COUNT(*) AS location_count,
            COUNT(DISTINCT sub_category) AS category_diversity
        FROM {table_name}
        WHERE {RETAIL_FILTER}
            AND {OPEN_FILTER}
            AND city IS NOT NULL
            AND state IS NOT NULL
        GROUP BY city, state
    )
    SELECT 
        city,
        state,
        location_count,
        category_diversity,
        ROUND(category_diversity * 1.0 / location_count, 2) AS diversity_ratio
    FROM city_coverage
    ORDER BY location_count DESC
    """

def get_data_completeness_query(table_name: str) -> str:
    """Get optimized query for data completeness assessment."""
    return f"""
    SELECT 
        'name' AS field,
        COUNT(*) AS total_records,
        COUNT(*) FILTER (WHERE name IS NOT NULL AND name != '') AS complete_records,
        ROUND(COUNT(*) FILTER (WHERE name IS NOT NULL AND name != '') * 100.0 / COUNT(*), 2) AS completeness_percentage
    FROM {table_name}
    WHERE {RETAIL_FILTER}
        AND {OPEN_FILTER}
    
    UNION ALL
    
    SELECT 
        'address' AS field,
        COUNT(*) AS total_records,
        COUNT(*) FILTER (WHERE address IS NOT NULL AND address != '') AS complete_records,
        ROUND(COUNT(*) FILTER (WHERE address IS NOT NULL AND address != '') * 100.0 / COUNT(*), 2) AS completeness_percentage
    FROM {table_name}
    WHERE {RETAIL_FILTER}
        AND {OPEN_FILTER}
    
    UNION ALL
    
    SELECT 
        'phone' AS field,
        COUNT(*) AS total_records,
        COUNT(*) FILTER (WHERE phone IS NOT NULL AND phone != '') AS complete_records,
        ROUND(COUNT(*) FILTER (WHERE phone IS NOT NULL AND phone != '') * 100.0 / COUNT(*), 2) AS completeness_percentage
    FROM {table_name}
    WHERE {RETAIL_FILTER}
        AND {OPEN_FILTER}
    
    UNION ALL
    
    SELECT 
        'coordinates' AS field,
        COUNT(*) AS total_records,
        COUNT(*) FILTER (WHERE latitude IS NOT NULL AND longitude IS NOT NULL) AS complete_records,
        ROUND(COUNT(*) FILTER (WHERE latitude IS NOT NULL AND longitude IS NOT NULL) * 100.0 / COUNT(*), 2) AS completeness_percentage
    FROM {table_name}
    WHERE {RETAIL_FILTER}
        AND {OPEN_FILTER}
    
    ORDER BY completeness_percentage ASC
    """
