import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns
import polars as pl
import duckdb
from util.database import get_tables

# Import the CPG analysis module
from util import cpg_analysis

def display_cpg_analysis_queries():
    """Display CPG analysis queries in a tabbed interface with explanations."""
    
    st.markdown("## CPG Data Analysis Queries")
    st.markdown("""
    This section provides specialized queries and analyses for Consumer Packaged Goods (CPG) data.
    Each query focuses on a specific aspect of CPG business operations and market analysis.
    """)
    
    # Create tabs for different query categories
    query_tabs = st.tabs([
        "Chain Store Analysis", 
        "Data Completeness", 
        "Territory Coverage",
        "Delivery Windows",
        "Retail Segments",
        "Competitive Density"
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
        
        **Business Value:**
        - Prioritize sales efforts on chains with the most locations
        - Identify regional vs. national chain presence
        - Assess data quality across chain locations
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
    
    # Tab 2: Data Completeness Analysis
    with query_tabs[1]:
        st.markdown("### Data Completeness Analysis")
        st.markdown("""
        This analysis assesses the completeness of critical data fields essential for CPG operations.
        Complete and accurate data is crucial for effective distribution planning and retail execution.
        
        **Business Value:**
        - Identify data quality gaps that could impact operations
        - Prioritize data collection efforts for critical fields
        - Track data quality improvements over time
        """)
        
        if st.button("Run Data Completeness Analysis"):
            with st.spinner("Analyzing data completeness..."):
                try:
                    # Run the analysis
                    completeness = cpg_analysis.assess_critical_data_completeness(conn, selected_table)
                    
                    if completeness.empty:
                        st.info("No data completeness information available.")
                    else:
                        # Extract metrics
                        missing_address_pct = float(completeness['missing_address_pct'].iloc[0])
                        missing_hours_pct = float(completeness['missing_hours_pct'].iloc[0])
                        missing_website_pct = float(completeness['missing_website_pct'].iloc[0])
                        low_confidence_pct = float(completeness['low_confidence_pct'].iloc[0])
                        
                        # Display metrics
                        st.markdown("#### Data Quality Metrics")
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.metric("Missing Addresses", f"{missing_address_pct:.1f}%", 
                                     delta="-9.3%" if missing_address_pct < 9.3 else None)
                            st.metric("Missing Hours", f"{missing_hours_pct:.1f}%", 
                                     delta="-76.7%" if missing_hours_pct < 76.7 else None)
                        
                        with col2:
                            st.metric("Missing Websites", f"{missing_website_pct:.1f}%", 
                                     delta="-27.2%" if missing_website_pct < 27.2 else None)
                            st.metric("Low Confidence Records", f"{low_confidence_pct:.1f}%", 
                                     delta="-37.3%" if low_confidence_pct < 37.3 else None)
                        
                        # Visualize data completeness
                        st.markdown("#### Data Completeness Visualization")
                        
                        # Create visualization
                        fig, ax = plt.subplots(figsize=(10, 6))
                        categories = ['Addresses', 'Hours', 'Websites', 'High Confidence']
                        complete_values = [100 - missing_address_pct, 100 - missing_hours_pct, 
                                          100 - missing_website_pct, 100 - low_confidence_pct]
                        missing_values = [missing_address_pct, missing_hours_pct, 
                                         missing_website_pct, low_confidence_pct]
                        
                        bar_width = 0.35
                        index = range(len(categories))
                        
                        ax.bar(index, complete_values, bar_width, label='Complete', color='green')
                        ax.bar(index, missing_values, bar_width, bottom=complete_values, label='Missing', color='red')
                        
                        ax.set_xlabel('Data Category')
                        ax.set_ylabel('Percentage')
                        ax.set_title('Data Completeness Assessment')
                        ax.set_xticks(index)
                        ax.set_xticklabels(categories)
                        ax.set_ylim(0, 100)
                        ax.legend()
                        plt.tight_layout()
                        
                        # Display the plot
                        st.pyplot(fig)
                        
                except Exception as e:
                    st.error(f"Error running data completeness analysis: {str(e)}")
    
    # Tab 3: Territory Coverage Analysis
    with query_tabs[2]:
        st.markdown("### Territory Coverage Analysis")
        st.markdown("""
        This analysis maps retail distribution by city for sales territory planning. It helps identify
        high-density areas for focused sales efforts and areas with limited coverage that may need attention.
        
        **Business Value:**
        - Optimize sales territory assignments
        - Identify expansion opportunities in underserved areas
        - Balance workload across sales territories
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
                        top_territories = territories.head(10)
                        
                        # Create a stacked bar chart
                        ax.bar(top_territories['city'], top_territories['retail_locations'], 
                              label='Retail', color='skyblue')
                        ax.bar(top_territories['city'], top_territories['grocery_locations'], 
                              bottom=top_territories['retail_locations'], label='Grocery', color='orange')
                        
                        ax.set_title('Retail and Grocery Distribution by City')
                        ax.set_xlabel('City')
                        ax.set_ylabel('Number of Locations')
                        plt.xticks(rotation=45, ha='right')
                        ax.legend()
                        plt.tight_layout()
                        
                        # Display the plot
                        st.pyplot(fig)
                        
                except Exception as e:
                    st.error(f"Error running territory coverage analysis: {str(e)}")
    
    # Tab 4: Delivery Windows Analysis
    with query_tabs[3]:
        st.markdown("### Delivery Windows Analysis")
        st.markdown("""
        This analysis examines business hours to identify optimal delivery windows for CPG products.
        Efficient delivery scheduling is critical for maximizing store coverage while minimizing logistics costs.
        
        **Business Value:**
        - Optimize delivery routes and schedules
        - Identify stores with extended or limited receiving hours
        - Plan staffing for delivery operations
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
    
    # Tab 5: Retail Segments Analysis
    with query_tabs[4]:
        st.markdown("### Retail Segments Analysis")
        st.markdown("""
        This analysis examines the distribution of retail categories to identify key segments for CPG products.
        Understanding the retail landscape helps target the right channels for specific product types.
        
        **Business Value:**
        - Identify primary retail channels for product placement
        - Discover niche retail segments for specialty products
        - Track changes in retail composition over time
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
                        
                        # Create a pie chart of top segments
                        top_segments = segments.head(8)
                        ax.pie(top_segments['location_count'], labels=top_segments['sub_category'], 
                              autopct='%1.1f%%', startangle=90)
                        ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle
                        ax.set_title('Distribution of Top Retail Segments')
                        plt.tight_layout()
                        
                        # Display the plot
                        st.pyplot(fig)
                        
                except Exception as e:
                    st.error(f"Error running retail segments analysis: {str(e)}")
    
    # Tab 6: Competitive Density Analysis
    with query_tabs[5]:
        st.markdown("### Competitive Density Analysis")
        st.markdown("""
        This analysis maps retail density by postal code to identify areas of high competitive activity.
        Understanding competitive density helps optimize distribution and marketing strategies.
        
        **Business Value:**
        - Identify high-opportunity areas for focused distribution
        - Avoid oversaturated markets with excessive competition
        - Target marketing efforts in competitive hotspots
        """)
        
        top_n = st.slider("Number of top postal codes to analyze:", 5, 30, 10)
        
        if st.button("Run Competitive Density Analysis"):
            with st.spinner("Analyzing competitive density..."):
                try:
                    # Run the analysis
                    density = cpg_analysis.analyze_competitive_density(conn, selected_table, top_n)
                    
                    if density.empty:
                        st.info("No competitive density information available.")
                    else:
                        st.success(f"Analyzed competitive density across {len(density)} postal codes")
                        
                        # Display results
                        st.dataframe(density, use_container_width=True)
                        
                        # Visualize competitive density
                        st.markdown("### Retail Density by Postal Code")
                        
                        # Create visualization
                        fig, ax = plt.subplots(figsize=(12, 6))
                        
                        # Convert to polars DataFrame if it's not already
                        if not isinstance(density, pl.DataFrame):
                            density = pl.from_pandas(density)
                        
                        # Convert to pandas for seaborn
                        density_pd = density.to_pandas()
                        
                        # Use seaborn for visualization
                        sns.barplot(x='postal_code', y='location_count', data=density_pd, ax=ax)
                        
                        ax.set_title('Retail Location Density by Postal Code')
                        ax.set_xlabel('Postal Code')
                        ax.set_ylabel('Number of Locations')
                        plt.xticks(rotation=45, ha='right')
                        plt.tight_layout()
                        
                        # Display the plot
                        st.pyplot(fig)
                        
                except Exception as e:
                    st.error(f"Error running competitive density analysis: {str(e)}")
