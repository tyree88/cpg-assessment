import streamlit as st
import matplotlib.pyplot as plt
import duckdb
import numpy as np

def render_distribution_quality_tab(conn, table_name):
    """Render the Distribution Quality metrics tab."""
    st.markdown("### Distribution Quality Metrics")
    st.markdown("""
    **Business Impact**: Distribution quality directly impacts product availability and sales performance. 
    Poor distribution data can lead to missed opportunities, inefficient routing, and wasted resources.
    
    This analysis identifies the quality of distribution points by examining:
    - Completeness of address and location data
    - Accuracy of operating hours information
    - Confidence scores for retail and grocery locations
    """)
    
    # Run distribution quality query
    try:
        query = f"""
        SELECT 
            main_category,
            COUNT(*) AS total_locations,
            ROUND(AVG(data_quality_confidence_score) * 100, 1) AS avg_confidence_score,
            SUM(CASE WHEN address IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_address_pct,
            SUM(CASE WHEN monday_open IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_hours_pct,
            SUM(CASE WHEN data_quality_confidence_score < 0.7 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS low_confidence_pct
        FROM {table_name}
        WHERE main_category IN ('retail', 'convenience_and_grocery_stores')
            AND open_closed_status = 'open'
        GROUP BY main_category
        ORDER BY total_locations DESC
        """
        
        results = conn.execute(query).df()
        
        if not results.empty:
            # Display metrics
            st.dataframe(results, use_container_width=True)
            
            # Create visualization
            st.markdown("#### Data Quality by Category")
            fig, ax = plt.subplots(figsize=(10, 6))
            
            # Prepare data for plotting
            categories = results['main_category'].tolist()
            metrics = [
                results['missing_address_pct'].tolist(),
                results['missing_hours_pct'].tolist(),
                results['low_confidence_pct'].tolist()
            ]
            
            x = range(len(categories))
            width = 0.25
            
            # Create grouped bar chart
            ax.bar([i - width for i in x], metrics[0], width, label='Missing Address %')
            ax.bar(x, metrics[1], width, label='Missing Hours %')
            ax.bar([i + width for i in x], metrics[2], width, label='Low Confidence %')
            
            ax.set_ylabel('Percentage')
            ax.set_title('Distribution Data Quality Issues by Category')
            ax.set_xticks(x)
            ax.set_xticklabels(categories)
            ax.legend()
            
            st.pyplot(fig)
            
            # Add quality assessment
            st.markdown("#### Quality Assessment")
            for idx, row in results.iterrows():
                category = row['main_category']
                missing_address = row['missing_address_pct']
                missing_hours = row['missing_hours_pct']
                low_confidence = row['low_confidence_pct']
                
                if missing_address > 10 or missing_hours > 50 or low_confidence > 30:
                    st.markdown(f"<p class='critical-issue'>⚠️ <strong>{category}</strong>: Critical data quality issues detected</p>", unsafe_allow_html=True)
                elif missing_address > 5 or missing_hours > 30 or low_confidence > 20:
                    st.markdown(f"<p class='warning-issue'>⚠️ <strong>{category}</strong>: Moderate data quality issues detected</p>", unsafe_allow_html=True)
                else:
                    st.markdown(f"<p class='good-quality'>✅ <strong>{category}</strong>: Good data quality</p>", unsafe_allow_html=True)
        else:
            st.info("No distribution data available for analysis.")
    except Exception as e:
        st.error(f"Error running distribution quality query: {str(e)}")


def render_chain_store_metrics_tab(conn, table_name):
    """Render the Chain Store Quality metrics tab."""
    st.markdown("### Chain Store Quality Metrics")
    st.markdown("""
    **Business Impact**: Chain stores represent high-value distribution opportunities. 
    A single agreement with a chain can open multiple distribution points, but data quality 
    inconsistencies across chain locations can lead to execution problems.
    
    This analysis identifies:
    - Data quality variations across chain locations
    - Chains with inconsistent or incomplete data
    - High-priority chains for data quality improvement
    """)
    
    # Run chain store quality query
    try:
        query = f"""
        SELECT 
            chain_name,
            COUNT(*) AS location_count,
            STRING_AGG(DISTINCT city, ', ') AS cities,
            MIN(data_quality_confidence_score) AS min_confidence,
            MAX(data_quality_confidence_score) AS max_confidence,
            AVG(data_quality_confidence_score) AS avg_confidence,
            (MAX(data_quality_confidence_score) - MIN(data_quality_confidence_score)) AS confidence_range
        FROM {table_name}
        WHERE main_category IN ('retail', 'convenience_and_grocery_stores')
            AND open_closed_status = 'open'
            AND chain_id IS NOT NULL
            AND chain_id != ''
        GROUP BY chain_name
        HAVING COUNT(*) >= 3
        ORDER BY confidence_range DESC, location_count DESC
        LIMIT 10
        """
        
        results = conn.execute(query).df()
        
        if not results.empty:
            # Display metrics
            st.dataframe(results, use_container_width=True)
            
            # Create visualization
            st.markdown("#### Data Quality Consistency Across Chains")
            fig, ax = plt.subplots(figsize=(10, 6))
            
            # Prepare data for plotting
            chains = results['chain_name'].tolist()
            min_conf = results['min_confidence'].tolist()
            max_conf = results['max_confidence'].tolist()
            avg_conf = results['avg_confidence'].tolist()
            
            # Limit to top 7 for readability
            if len(chains) > 7:
                chains = chains[:7]
                min_conf = min_conf[:7]
                max_conf = max_conf[:7]
                avg_conf = avg_conf[:7]
                
            x = range(len(chains))
            
            # Create line and scatter plot
            for i in x:
                ax.plot([i, i], [min_conf[i], max_conf[i]], 'r-', alpha=0.5)
            
            ax.scatter(x, min_conf, color='red', label='Min Confidence')
            ax.scatter(x, max_conf, color='green', label='Max Confidence')
            ax.scatter(x, avg_conf, color='blue', label='Avg Confidence')
            
            ax.set_ylabel('Confidence Score')
            ax.set_title('Data Quality Consistency Across Chain Locations')
            ax.set_xticks(x)
            ax.set_xticklabels(chains, rotation=45, ha='right')
            ax.legend()
            
            plt.tight_layout()
            st.pyplot(fig)
            
            # Add quality assessment
            st.markdown("#### Quality Assessment")
            for idx, row in results.iterrows():
                chain = row['chain_name']
                conf_range = row['confidence_range']
                min_conf = row['min_confidence']
                
                if conf_range > 0.3 or min_conf < 0.5:
                    st.markdown(f"<p class='critical-issue'>⚠️ <strong>{chain}</strong>: High data quality inconsistency (range: {conf_range:.2f})</p>", unsafe_allow_html=True)
                elif conf_range > 0.2 or min_conf < 0.7:
                    st.markdown(f"<p class='warning-issue'>⚠️ <strong>{chain}</strong>: Moderate data quality inconsistency (range: {conf_range:.2f})</p>", unsafe_allow_html=True)
                else:
                    st.markdown(f"<p class='good-quality'>✅ <strong>{chain}</strong>: Consistent data quality (range: {conf_range:.2f})</p>", unsafe_allow_html=True)
        else:
            st.info("No chain store data available for analysis.")
    except Exception as e:
        st.error(f"Error running chain store quality query: {str(e)}")


def render_geographic_coverage_tab(conn, table_name):
    """Render the Geographic Coverage Quality tab."""
    st.markdown("### Geographic Coverage Quality")
    st.markdown("""
    **Business Impact**: Geographic coverage quality affects territory planning, market penetration, 
    and sales resource allocation. Incomplete or inaccurate geographic data can lead to inefficient 
    territory assignments and missed market opportunities.
    
    This analysis identifies:
    - Geographic distribution of data quality issues
    - Areas with incomplete location data
    - Regions requiring data quality improvement
    """)
    
    # Run geographic coverage query
    try:
        query = f"""
        SELECT 
            city,
            COUNT(*) AS total_locations,
            ROUND(AVG(data_quality_confidence_score) * 100, 1) AS avg_confidence_score,
            SUM(CASE WHEN latitude IS NULL OR longitude IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_coordinates_pct,
            SUM(CASE WHEN address IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_address_pct,
            SUM(CASE WHEN data_quality_confidence_score < 0.7 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS low_confidence_pct
        FROM {table_name}
        WHERE open_closed_status = 'open'
        GROUP BY city
        HAVING COUNT(*) >= 10
        ORDER BY avg_confidence_score
        LIMIT 10
        """
        
        results = conn.execute(query).df()
        
        if not results.empty:
            # Display metrics
            st.dataframe(results, use_container_width=True)
            
            # Create visualization
            st.markdown("#### Geographic Data Quality Issues")
            fig, ax = plt.subplots(figsize=(10, 6))
            
            # Prepare data for plotting
            cities = results['city'].tolist()
            missing_coords = results['missing_coordinates_pct'].tolist()
            missing_addr = results['missing_address_pct'].tolist()
            low_conf = results['low_confidence_pct'].tolist()
            
            # Limit to top 7 for readability
            if len(cities) > 7:
                cities = cities[:7]
                missing_coords = missing_coords[:7]
                missing_addr = missing_addr[:7]
                low_conf = low_conf[:7]
                
            x = range(len(cities))
            width = 0.25
            
            # Create grouped bar chart
            ax.bar([i - width for i in x], missing_coords, width, label='Missing Coordinates %')
            ax.bar(x, missing_addr, width, label='Missing Address %')
            ax.bar([i + width for i in x], low_conf, width, label='Low Confidence %')
            
            ax.set_ylabel('Percentage')
            ax.set_title('Geographic Data Quality Issues by City')
            ax.set_xticks(x)
            ax.set_xticklabels(cities, rotation=45, ha='right')
            ax.legend()
            
            plt.tight_layout()
            st.pyplot(fig)
            
            # Add quality assessment
            st.markdown("#### Quality Assessment")
            for idx, row in results.iterrows():
                city = row['city']
                missing_coords = row['missing_coordinates_pct']
                missing_addr = row['missing_address_pct']
                low_conf = row['low_confidence_pct']
                
                if missing_coords > 10 or missing_addr > 10 or low_conf > 30:
                    st.markdown(f"<p class='critical-issue'>⚠️ <strong>{city}</strong>: Critical geographic data quality issues</p>", unsafe_allow_html=True)
                elif missing_coords > 5 or missing_addr > 5 or low_conf > 20:
                    st.markdown(f"<p class='warning-issue'>⚠️ <strong>{city}</strong>: Moderate geographic data quality issues</p>", unsafe_allow_html=True)
                else:
                    st.markdown(f"<p class='good-quality'>✅ <strong>{city}</strong>: Good geographic data quality</p>", unsafe_allow_html=True)
        else:
            st.info("No geographic coverage data available for analysis.")
    except Exception as e:
        st.error(f"Error running geographic coverage query: {str(e)}")


def render_data_completeness_tab(conn, table_name):
    """Render the Critical Field Completeness tab."""
    st.markdown("### Critical Field Completeness")
    st.markdown("""
    **Business Impact**: Missing critical data fields can severely impact business operations, 
    from distribution planning to customer engagement. Incomplete data leads to inefficiencies, 
    missed opportunities, and poor decision-making.
    
    This analysis identifies:
    - Completeness of critical business fields
    - Fields with the highest missing data rates
    - Impact of missing data on business operations
    """)
    
    # Run data completeness query
    try:
        query = f"""
        SELECT 
            'Contact Information' AS data_category,
            COUNT(*) AS total_records,
            SUM(CASE WHEN phone IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_phone_pct,
            SUM(CASE WHEN website IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_website_pct,
            SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_email_pct
        FROM {table_name}
        WHERE open_closed_status = 'open'
        UNION ALL
        SELECT 
            'Location Data' AS data_category,
            COUNT(*) AS total_records,
            SUM(CASE WHEN address IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_address_pct,
            SUM(CASE WHEN latitude IS NULL OR longitude IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_coordinates_pct,
            SUM(CASE WHEN postal_code IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_postal_pct
        FROM {table_name}
        WHERE open_closed_status = 'open'
        UNION ALL
        SELECT 
            'Business Information' AS data_category,
            COUNT(*) AS total_records,
            SUM(CASE WHEN main_category IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_category_pct,
            SUM(CASE WHEN monday_open IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_hours_pct,
            SUM(CASE WHEN opened_on IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_opened_date_pct
        FROM {table_name}
        WHERE open_closed_status = 'open'
        """
        
        results = conn.execute(query).df()
        
        if not results.empty:
            # Display metrics
            st.dataframe(results, use_container_width=True)
            
            # Create visualization
            st.markdown("#### Critical Field Completeness")
            fig, ax = plt.subplots(figsize=(12, 6))
            
            # Reshape data for visualization
            categories = results['data_category'].tolist()
            metrics_data = {}
            
            # Get all column names except the first two
            metric_columns = results.columns[2:]
            
            for col in metric_columns:
                metrics_data[col] = results[col].tolist()
            
            # Set up the plot
            x = range(len(categories))
            width = 0.2
            offset_multiplier = -1 * (len(metric_columns) - 1) / 2
            
            # Plot each metric as a bar
            for i, (metric_name, values) in enumerate(metrics_data.items()):
                offset = offset_multiplier + i
                display_name = metric_name.replace('missing_', '').replace('_pct', '')
                ax.bar([pos + (width * offset) for pos in x], values, width, label=display_name)
            
            ax.set_ylabel('Missing Data Percentage')
            ax.set_title('Critical Field Completeness by Category')
            ax.set_xticks(x)
            ax.set_xticklabels(categories)
            ax.legend()
            
            plt.tight_layout()
            st.pyplot(fig)
            
            # Add quality assessment
            st.markdown("#### Quality Assessment")
            
            # Calculate average missing data percentage for each category
            for idx, row in results.iterrows():
                category = row['data_category']
                metrics = row.iloc[2:]
                avg_missing = metrics.mean()
                max_missing = metrics.max()
                worst_field = metrics.idxmax().replace('missing_', '').replace('_pct', '')
                
                if max_missing > 50:
                    st.markdown(f"<p class='critical-issue'>⚠️ <strong>{category}</strong>: Critical data completeness issues (avg: {avg_missing:.1f}%, worst: {worst_field} at {max_missing:.1f}%)</p>", unsafe_allow_html=True)
                elif max_missing > 25:
                    st.markdown(f"<p class='warning-issue'>⚠️ <strong>{category}</strong>: Moderate data completeness issues (avg: {avg_missing:.1f}%, worst: {worst_field} at {max_missing:.1f}%)</p>", unsafe_allow_html=True)
                else:
                    st.markdown(f"<p class='good-quality'>✅ <strong>{category}</strong>: Good data completeness (avg: {avg_missing:.1f}%, worst: {worst_field} at {max_missing:.1f}%)</p>", unsafe_allow_html=True)
        else:
            st.info("No data completeness information available.")
    except Exception as e:
        st.error(f"Error running data completeness query: {str(e)}")


def render_category_consistency_tab(conn, table_name):
    """Render the Category Hierarchy Consistency tab."""
    st.markdown("### Category Hierarchy Consistency")
    st.markdown("""
    **Business Impact**: Inconsistent category hierarchies create challenges for product placement, 
    merchandising, and market analysis. They can lead to incorrect product categorization, 
    misaligned marketing strategies, and inaccurate competitive analysis.
    
    This analysis identifies:
    - Inconsistencies in category hierarchies
    - Misaligned category-subcategory relationships
    - Potential data standardization issues
    """)
    
    # Run category consistency query
    try:
        query = f"""
        WITH category_mapping AS (
            SELECT 
                main_category,
                sub_category,
                COUNT(*) AS record_count
            FROM {table_name}
            WHERE main_category IS NOT NULL 
              AND sub_category IS NOT NULL
            GROUP BY main_category, sub_category
        ),
        category_stats AS (
            SELECT
                main_category,
                COUNT(DISTINCT sub_category) AS subcategory_count,
                SUM(record_count) AS total_records
            FROM category_mapping
            GROUP BY main_category
        )
        SELECT
            cm.main_category,
            cm.sub_category,
            cm.record_count,
            cs.subcategory_count,
            ROUND(cm.record_count * 100.0 / cs.total_records, 1) AS pct_of_category
        FROM category_mapping cm
        JOIN category_stats cs ON cm.main_category = cs.main_category
        WHERE cs.subcategory_count > 1
        ORDER BY cs.subcategory_count DESC, cm.main_category, cm.record_count DESC
        """
        
        results = conn.execute(query).df()
        
        if not results.empty:
            # Get unique main categories
            main_categories = results['main_category'].unique()
            
            # Display dropdown to select category
            selected_category = st.selectbox(
                "Select main category to analyze:", 
                options=main_categories,
                index=0,
                key="category_consistency_select"
            )
            
            # Filter results for selected category
            category_results = results[results['main_category'] == selected_category]
            
            # Display metrics
            st.dataframe(category_results, use_container_width=True)
            
            # Create visualization
            st.markdown(f"#### Sub-Category Distribution for '{selected_category}'")
            fig, ax = plt.subplots(figsize=(10, 6))
            
            # Prepare data for plotting
            subcategories = category_results['sub_category'].tolist()
            record_counts = category_results['record_count'].tolist()
            
            # Limit to top 10 for readability
            if len(subcategories) > 10:
                subcategories = subcategories[:10]
                record_counts = record_counts[:10]
            
            # Create bar chart
            bars = ax.bar(subcategories, record_counts)
            
            # Add data labels
            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                        f'{height:.0f}', ha='center', va='bottom', rotation=0)
            
            ax.set_ylabel('Record Count')
            ax.set_title(f'Sub-Category Distribution for {selected_category}')
            plt.xticks(rotation=45, ha='right')
            
            plt.tight_layout()
            st.pyplot(fig)
            
            # Add quality assessment
            st.markdown("#### Consistency Assessment")
            subcategory_count = category_results['subcategory_count'].iloc[0]
            
            # Calculate entropy/distribution metrics
            total_records = sum(record_counts)
            percentages = [count/total_records for count in record_counts]
            min_pct = min(percentages) * 100
            
            if subcategory_count > 10 and min_pct < 1:
                st.markdown(f"<p class='critical-issue'>⚠️ <strong>{selected_category}</strong>: High category inconsistency ({subcategory_count} subcategories, smallest is {min_pct:.1f}% of records)</p>", unsafe_allow_html=True)
            elif subcategory_count > 5 and min_pct < 5:
                st.markdown(f"<p class='warning-issue'>⚠️ <strong>{selected_category}</strong>: Moderate category inconsistency ({subcategory_count} subcategories, smallest is {min_pct:.1f}% of records)</p>", unsafe_allow_html=True)
            else:
                st.markdown(f"<p class='good-quality'>✅ <strong>{selected_category}</strong>: Reasonable category consistency ({subcategory_count} subcategories)</p>", unsafe_allow_html=True)
            
            # Recommendations
            st.markdown("#### Recommendations")
            if subcategory_count > 10:
                st.markdown("""
                - Consider standardizing subcategories to reduce fragmentation
                - Merge similar subcategories to improve consistency
                - Implement data validation rules for category assignments
                - Review outlier subcategories with very few records
                """)
            elif subcategory_count > 5:
                st.markdown("""
                - Review subcategory naming conventions for consistency
                - Consider consolidating similar subcategories
                - Implement validation for new category assignments
                """)
            else:
                st.markdown("""
                - Current category hierarchy appears reasonable
                - Continue monitoring for consistency
                - Document category definitions to maintain consistency
                """)
        else:
            st.info("No category hierarchy data available for analysis.")
    except Exception as e:
        st.error(f"Error running category consistency query: {str(e)}")


def render_cpg_metrics_tabs():
    """Render the CPG Data Quality Metrics tab with specialized CPG queries and visualizations."""
    st.markdown('<h2 class="sub-header">CPG Data Quality Metrics</h2>', unsafe_allow_html=True)
    
    # Add custom CSS to make tabs display in a single row
    st.markdown("""
    <style>
    div[data-testid="stHorizontalBlock"] > div:first-child {overflow-x: auto;}
    div[role="tablist"] {flex-wrap: nowrap !important;}
    button[role="tab"] {min-width: auto !important; padding-left: 10px !important; padding-right: 10px !important;}
    </style>
    """, unsafe_allow_html=True)
    
    # Check if data is loaded
    if st.session_state.df is not None and st.session_state.table_name is not None:
        # Create tabs for different CPG-specific metrics
        cpg_metrics_tabs = st.tabs([
            "Distribution Quality", 
            "Chain Store Metrics", 
            "Geographic Coverage", 
            "Data Completeness", 
            "Category Consistency"
        ])
        
        # Try to connect to the database
        try:
            conn = duckdb.connect('md:my_db')
            table_name = st.session_state.table_name
            
            # Tab 1: Distribution Quality
            with cpg_metrics_tabs[0]:
                render_distribution_quality_tab(conn, table_name)
            
            # Tab 2: Chain Store Metrics
            with cpg_metrics_tabs[1]:
                render_chain_store_metrics_tab(conn, table_name)
            
            # Tab 3: Geographic Coverage
            with cpg_metrics_tabs[2]:
                render_geographic_coverage_tab(conn, table_name)
            
            # Tab 4: Data Completeness
            with cpg_metrics_tabs[3]:
                render_data_completeness_tab(conn, table_name)
            
            # Tab 5: Category Consistency
            with cpg_metrics_tabs[4]:
                render_category_consistency_tab(conn, table_name)
                
        except Exception as e:
            st.error(f"Error connecting to database: {str(e)}")
    else:
        st.info("Please load data first to run CPG data quality metrics.")
