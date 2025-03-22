import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns
import polars as pl
import duckdb
from util.database import get_tables
import pandas as pd

# Import the CPG analysis module
from util import cpg_analysis

def display_cpg_analysis_queries():
    """Display CPG analysis queries in a tabbed interface with explanations."""
    
    st.markdown("## CPG Data Analysis Queries")
    st.warning("""⚠️ **Known Issue**: When you run a query it will take you back to the home tab. 
               Continue back to the query tab to continue analysis! This is issue still being ironed out""")
    st.markdown("""
    This section provides specialized queries and analyses for Consumer Packaged Goods (CPG) data.
    Each query focuses on a specific aspect of CPG business operations and market analysis.
    """)
    
    # Create tabs for different query categories
    query_tabs = st.tabs([
        "Chain Store Analysis", 
        "Territory Coverage",
        "Delivery Windows",
        "Retail Segments"
    ])
    
    # Get the current database connection
    try:
        conn = duckdb.connect('md:my_db')
        
        # Get available tables
        tables = get_tables()
        
        if not tables:
            st.warning("No tables available in the database. Please load data first.")
            return
            
        # Select which table to analyze
        selected_table = st.selectbox("Select a table to analyze:", tables)
    except Exception as e:
        st.error(f"Error connecting to database: {str(e)}")
        return
    
    # Tab 1: Chain Store Analysis
    with query_tabs[0]:
        st.markdown("### Chain Store Analysis")
        st.markdown("""
        This analysis identifies retail chains with multiple locations, which are ideal targets for efficient 
        distribution deals. Working with chains allows CPG companies to negotiate single agreements that 
        cover multiple retail locations.
        
        **Query Being Executed:**
        ```sql
        SELECT 
            business_name as chain_name,
            COUNT(*) as location_count,
            COUNT(DISTINCT postal_code) as postal_codes_covered,
            COUNT(DISTINCT city) as cities_covered,
            ROUND(AVG(confidence_score), 2) as avg_confidence
        FROM locations
        GROUP BY business_name
        HAVING COUNT(*) >= {min_locations}
        ORDER BY location_count DESC;
        ```
        
        **What This Query Does:**
        - Counts locations per business name
        - Tracks geographic spread (postal codes and cities)
        - Calculates average data confidence
        - Filters for chains with minimum location count
        """)
        
        min_locations = st.slider("Minimum locations per chain:", 2, 20, 3)
        
        if st.button("Run Chain Store Analysis"):
            with st.spinner("Analyzing chain stores..."):
                try:
                    # Run the analysis
                    chains = cpg_analysis.identify_chain_store_targets(conn, selected_table, min_locations)
                    
                    if chains.empty:
                        st.info("No chains found with the specified criteria.")
                    else:
                        st.success(f"Found {len(chains)} chains with {min_locations}+ locations")
                        
                        # Display results
                        st.dataframe(chains, use_container_width=True)
                        
                        # Visualize top chains
                        st.markdown("### Top Retail Chains")
                        
                        # Create visualization
                        fig, ax = plt.subplots(figsize=(10, 6))
                        
                        # Convert to polars DataFrame if it's not already
                        if not isinstance(chains, pl.DataFrame):
                            chains = pl.from_pandas(chains)
                            
                        # Get top chains and convert to pandas for seaborn
                        top_chains = chains.sort("location_count", descending=True).head(10).to_pandas()
                        
                        # Use seaborn for visualization
                        sns.barplot(x='location_count', y='chain_name', data=top_chains, ax=ax)
                        ax.set_title('Top Retail Chains for CPG Distribution')
                        ax.set_xlabel('Number of Locations')
                        ax.set_ylabel('Chain Name')
                        plt.tight_layout()
                        
                        # Display the plot
                        st.pyplot(fig)
                        
                except Exception as e:
                    st.error(f"Error running chain store analysis: {str(e)}")
    
    # Tab 2: Territory Coverage Analysis
    with query_tabs[1]:
        st.markdown("### Territory Coverage Analysis")
        st.markdown("""
        This analysis maps retail distribution by city for sales territory planning. It helps identify
        high-density areas for focused sales efforts and areas with limited coverage that may need attention.
        
        **Query Being Executed:**
        ```sql
        WITH city_coverage AS (
            SELECT 
                city,
                state,
                COUNT(*) AS location_count,
                COUNT(DISTINCT sub_category) AS category_diversity
            FROM locations
            WHERE main_category IN ('retail', 'convenience_and_grocery_stores')
                AND open_closed_status = 'open'
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
        ```
        
        **What This Query Does:**
        - Groups locations by city and state
        - Counts total locations per city
        - Measures category diversity (unique sub-categories)
        - Calculates diversity ratio (categories per location)
        """)
        
        if st.button("Run Territory Coverage Analysis"):
            with st.spinner("Analyzing territory coverage..."):
                try:
                    # Run the analysis
                    territories = cpg_analysis.analyze_territory_coverage(conn, selected_table)
                    
                    if territories.empty:
                        st.info("No territory coverage information available.")
                    else:
                        st.success(f"Analyzed {len(territories)} territories")
                        
                        # Display results
                        st.dataframe(territories, use_container_width=True)
                        
                        # Visualize territory coverage
                        st.markdown("### Retail Distribution by City")
                        
                        # Create visualization
                        fig, ax = plt.subplots(figsize=(12, 6))
                        
                        # Convert to polars DataFrame if needed
                        if not isinstance(territories, pl.DataFrame):
                            territories = pl.from_pandas(territories)
                            
                        # Get top 10 cities by location count
                        top_territories = territories.sort('location_count', descending=True).head(10).to_pandas()
                        
                        # Create a bar chart with location count and diversity
                        x = range(len(top_territories))
                        ax.bar(x, top_territories['location_count'], label='Total Locations', color='skyblue')
                        ax.set_xticks(x)
                        ax.set_xticklabels(top_territories['city'], rotation=45, ha='right')
                        
                        # Add category diversity as a line
                        ax2 = ax.twinx()
                        ax2.plot(x, top_territories['category_diversity'], color='red', marker='o', 
                                label='Category Diversity', linewidth=2)
                        
                        # Set labels and title
                        ax.set_xlabel('City')
                        ax.set_ylabel('Number of Locations')
                        ax2.set_ylabel('Category Diversity')
                        plt.title('Top Cities: Location Count vs Category Diversity')
                        
                        # Add legends
                        lines1, labels1 = ax.get_legend_handles_labels()
                        lines2, labels2 = ax2.get_legend_handles_labels()
                        ax.legend(lines1 + lines2, labels1 + labels2, loc='upper right')
                        
                        plt.tight_layout()
                        
                        # Display the plot
                        st.pyplot(fig)
                        
                except Exception as e:
                    st.error(f"Error running territory coverage analysis: {str(e)}")
                    st.error("Please check if all required columns exist in your data table.")
    
    # Tab 3: Delivery Windows Analysis
    with query_tabs[2]:
        st.markdown("### Delivery Windows Analysis")
        st.markdown("""
        This analysis examines business hours to identify optimal delivery windows for CPG products.
        Efficient delivery scheduling is critical for maximizing store coverage while minimizing logistics costs.
        
        **Query Being Executed:**
        ```sql
        SELECT 
            business_name,
            EXTRACT(HOUR FROM opening_time) as open_hour,
            EXTRACT(HOUR FROM closing_time) as close_hour,
            (EXTRACT(HOUR FROM closing_time) - EXTRACT(HOUR FROM opening_time)) as window_hours
        FROM locations
        WHERE day_of_week = '{selected_day}'
        AND opening_time IS NOT NULL
        AND closing_time IS NOT NULL
        ORDER BY window_hours DESC;
        ```
        
        **What This Query Does:**
        - Extracts business hours for selected day
        - Calculates delivery window duration
        - Identifies optimal delivery times
        - Flags extended-hours locations
        """)
        
        day_options = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        selected_day = st.selectbox("Select day of week:", day_options)
        
        if st.button("Run Delivery Windows Analysis"):
            with st.spinner(f"Analyzing delivery windows for {selected_day}..."):
                try:
                    # Run the analysis
                    windows = cpg_analysis.analyze_delivery_windows(conn, selected_table, selected_day.lower())
                    
                    if windows.empty:
                        st.info("No delivery window information available.")
                    else:
                        st.success(f"Analyzed delivery windows for {len(windows)} locations on {selected_day}")
                        
                        # Display results
                        st.dataframe(windows, use_container_width=True)
                        
                        # Visualize delivery windows
                        st.markdown(f"### Delivery Window Distribution for {selected_day}")
                        
                        # Create visualization
                        fig, ax = plt.subplots(figsize=(10, 6))
                        
                        # Convert to polars DataFrame if it's not already
                        if not isinstance(windows, pl.DataFrame):
                            windows = pl.from_pandas(windows)
                            
                        # Convert to pandas for seaborn
                        windows_pd = windows.to_pandas()
                        
                        # Use seaborn for visualization
                        sns.histplot(windows_pd['window_hours'], bins=12, kde=True, ax=ax)
                        
                        ax.set_title(f'Distribution of Delivery Window Hours on {selected_day}')
                        ax.set_xlabel('Window Hours')
                        ax.set_ylabel('Number of Locations')
                        plt.tight_layout()
                        
                        # Display the plot
                        st.pyplot(fig)
                        
                except Exception as e:
                    st.error(f"Error running delivery windows analysis: {str(e)}")
    
    # Tab 4: Retail Segments Analysis
    with query_tabs[3]:
        st.markdown("### Retail Segments Analysis")
        st.markdown("""
        This analysis examines the distribution of retail categories to identify key segments for CPG products.
        Understanding the retail landscape helps target the right channels for specific product types.
        
        **Query Being Executed:**
        ```sql
        WITH retail_categories AS (
            SELECT 
                sub_category AS category,
                COUNT(*) AS location_count
            FROM locations
            WHERE main_category IN ('retail', 'convenience_and_grocery_stores')
                AND open_closed_status = 'open'
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
        ```
        
        **What This Query Does:**
        - Groups locations by sub-category
        - Calculates total locations per category
        - Computes percentage distribution
        - Filters out invalid categories
        """)
        
        if st.button("Run Retail Segments Analysis"):
            with st.spinner("Analyzing retail segments..."):
                try:
                    # Run the analysis
                    segments = cpg_analysis.analyze_retail_segments(conn, selected_table)
                    
                    if segments.empty:
                        st.info("No retail segment information available.")
                    else:
                        st.success(f"Analyzed {len(segments)} retail segments")
                        
                        # Display results
                        st.dataframe(segments, use_container_width=True)
                        
                        # Visualize retail segments
                        st.markdown("### Retail Segment Distribution")
                        
                        # Create visualization
                        fig, ax = plt.subplots(figsize=(10, 6))
                        
                        # Convert to polars DataFrame if needed
                        if not isinstance(segments, pl.DataFrame):
                            segments = pl.from_pandas(segments)
                            
                        # Get top segments for visualization
                        top_segments = segments.head(8).to_pandas()
                        
                        # Create a pie chart of top segments
                        ax.pie(top_segments['location_count'], labels=top_segments['category'], 
                              autopct='%1.1f%%', startangle=90)
                        ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle
                        ax.set_title('Distribution of Top Retail Segments')
                        plt.tight_layout()
                        
                        # Display the plot
                        st.pyplot(fig)
                        
                except Exception as e:
                    st.error(f"Error running retail segments analysis: {str(e)}")
                    st.error("Please check if all required columns exist in your data table.")
