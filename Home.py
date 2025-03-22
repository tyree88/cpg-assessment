"""
DataPlor - CPG Data Quality Assessment

Main application file for the Streamlit-based data quality assessment tool.
This file has been refactored to use smaller, reusable components for better maintainability.
"""

import streamlit as st
import time
from typing import Dict

# Import utility modules
from util.analysis import analyze_data
from util.cleaning import identify_data_quality_issues, generate_cleaning_recommendations, clean_data
from util.styles import apply_all_styles

# Import components
from components.data_analysis import display_column_analysis, display_data_overview
from components.data_cleaning import display_data_completeness_options, display_deduplication_options, display_basic_cleaning_options
from components.data_report import render_data_quality_report
from components.ui_components import create_progress_steps, create_info_box
from components.cpg_queries import display_cpg_analysis_queries
from components.ui_helpers import (
    render_no_data_message, 
    render_key_metrics, 
    render_footer,
    render_welcome_section
)


def setup_page():
    """Configure the Streamlit page settings and apply custom CSS."""
    st.set_page_config(
        page_title="DataPlor - CPG Data Quality Assessment", 
        layout="wide",
        initial_sidebar_state="collapsed"
    )
    
    # Apply all styles from the styles module
    apply_all_styles()


def initialize_session_state():
    """Initialize session state variables if they don't exist."""
    # Data state variables
    if 'table_name' not in st.session_state:
        st.session_state.table_name = None
    if 'df' not in st.session_state:
        st.session_state.df = None
    if 'analysis' not in st.session_state:
        st.session_state.analysis = None
    if 'issues' not in st.session_state:
        st.session_state.issues = None
    
    # UI state variables for progressive disclosure
    if 'show_advanced_options' not in st.session_state:
        st.session_state.show_advanced_options = False
    if 'active_step' not in st.session_state:
        st.session_state.active_step = 1
    if 'completed_steps' not in st.session_state:
        st.session_state.completed_steps = set()
    if 'last_interaction' not in st.session_state:
        st.session_state.last_interaction = time.time()


def display_app_header():
    """Display the application header and title."""
    st.markdown('<h1 class="main-header">DataPlor - CPG Data Quality Assessment</h1>', unsafe_allow_html=True)


def render_overview():
    """Render the main overview page with key findings and getting started information."""
    st.markdown('<h2 class="sub-header">CPG Data Quality Assessment Overview</h2>', unsafe_allow_html=True)
    
    # Executive Summary Section
    st.markdown("### 📊 Executive Summary")
    st.markdown("""
    This report presents a comprehensive assessment of data quality for the point-of-interest (POI) and location data 
    stored in the client's database. The assessment reveals several critical data quality issues that require attention 
    to improve business decision-making and operational efficiency.

    The client's database contains **37,790 records** of business location data primarily in Idaho, with comprehensive 
    information covering business identifiers, categorization, location information, contact details, operational data, 
    and quality metrics.
    """)
    
    # Key Findings Section using columns
    st.markdown("### 🔍 Key Data Quality Issues")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Data Completeness Issues
        with st.container():
            st.markdown('<p class="critical-issue"><strong>Data Completeness Issues</strong></p>', unsafe_allow_html=True)
            st.markdown("""
            - 9.3% missing address data
            - 27.2% missing website information
            - 38.3% missing Monday business hours
            - Up to 76.7% missing Sunday business hours
            """)
        
        # Duplicate Records
        with st.container():
            st.markdown('<p class="warning-issue"><strong>Duplicate Records</strong></p>', unsafe_allow_html=True)
            st.markdown("""
            - 132 potential duplicate locations identified
            - Notable examples:
              - "Silvercreek Realty Group" (5 instances)
              - "Americana Terrace" (4 instances)
            """)
        
        # Data Confidence
        with st.container():
            st.markdown('<p class="warning-issue"><strong>Data Confidence Concerns</strong></p>', unsafe_allow_html=True)
            st.markdown("""
            - 37.3% of records have low confidence scores (below 0.7)
            - Only 5.7% have high confidence scores (above 0.9)
            - Average confidence score: 0.745
            """)
    
    with col2:
        # Category Issues
        with st.container():
            st.markdown('<p class="critical-issue"><strong>Category Inconsistencies</strong></p>', unsafe_allow_html=True)
            st.markdown("""
            - Potential misalignments in category hierarchies
            - Subcategories used incorrectly across main categories
            - Limited distinct full hierarchies despite many categories
            """)
        
        # Format Issues
        with st.container():
            st.markdown('<p class="warning-issue"><strong>Format Standardization Issues</strong></p>', unsafe_allow_html=True)
            st.markdown("""
            Postal code format inconsistencies:
            - 36,949 records: 5-digit format
            - 521 records: 9-digit format
            - 79 records: non-standard lengths
            """)
    
    # Business Impact Section
    st.markdown("### 💼 Business Impact")
    st.markdown("""
    Missing operational data (hours, websites) directly impacts customer experience and engagement. 
    Incomplete address information limits the utility of location-based analytics. Additionally:
    - Duplicates inflate location counts
    - Skewed market analysis
    - Customer confusion through inconsistent information
    """)
    
    # Business Operations Impact
    st.markdown("#### Critical Impact on CPG Operations")
    impact_col1, impact_col2 = st.columns(2)
    
    with impact_col1:
        st.markdown("""
        These issues directly affect the ability to:
        - Target retail distribution points
        - Plan delivery routes and schedules
        - Analyze competitive market landscapes
        """)
    
    with impact_col2:
        st.markdown("""
        Additional operational impacts:
        - Manage sales territories
        - Design consumer marketing campaigns
        - Optimize supply chain operations
        """)
    
    # Scalable Data Management Section
    st.markdown("### 🔄 Scalable Data Quality Management Framework")
    st.markdown("""
    The proposed framework for scalable data quality management features a three-layer architecture: Ingestion, Cleaning, and Distribution.

    It uses Airbyte for automated data collection, initially storing data in DuckDB before transitioning to ClickHouse for larger-scale analytics. Continuous data quality monitoring is handled by Soda, while Prefect manages the workflow with event orchestration and alerts. For data transformation, dbt is utilized, connected to Databricks for advanced analytics.

    The system operates on a hybrid cloud architecture using Google Kubernetes Engine, supporting scalability and consistent data quality, with capabilities for automated validation, real-time monitoring, and machine learning
    """)
    
    scale_col1, scale_col2, scale_col3 = st.columns(3)
    
    with scale_col1:
        st.markdown("""
        #### 🔍 Automated Validation Pipeline
        
        **Data Ingestion & Validation:**
        - Input validation at collection points using Airbye
        - Initial storage in DuckDB (POC phase)
        - Migration to ClickHouse for scaled analytics
        - Soda integration for quality monitoring
        
        **Pipeline Orchestration:**
        - Prefect for event orchestration
        - Real-time quality checks
        - Automated alerting system
        - Clear visibility dashboards
        """)
    
    with scale_col2:
        st.markdown("""
        #### 📊 Model Analysis & Development
        
        **Data Transformation:**
        - dbt for model development
        - Version-controlled transformations
        - Automated testing suite
        
        **Advanced Analytics:**
        - Databricks integration
        - Scalable computation
        - Machine learning capabilities
        - Automated model retraining
        """)
    
    with scale_col3:
        st.markdown("""
        #### 🚀 Scalable Architecture
        
        **Infrastructure:**
        - Hybrid cloud distributed system
        - GKE for model deployment
        - Auto-scaling capabilities
        
        **Data Flow:**
        - Streaming data processing
        - Batch processing for historical data
        - Real-time quality monitoring
        - Automated recovery procedures
        """)
    
    # Architecture Diagram
    st.markdown("#### System Architecture Overview")
    
    # Add Miro board link
    st.markdown("""
    > 🔗 [View interactive architecture diagram in Miro](https://miro.com/app/board/uXjVIMv-I3g=/?share_link_id=895951754429)
    """)
    
    # Create columns for better layout
    img_col1, img_col2 = st.columns([2, 1])
    
    with img_col1:
        # Add the detailed architecture image
        st.image("assets/architecture_diagram.jpg", 
                caption="Data Quality Pipeline Architecture",
                use_container_width=True)
    
    with img_col2:
        st.markdown("""
        #### Key Components:
        
        🔄 **Data Flow**
        - Data ingestion via Airbyte
        - Quality validation with Soda
        - Transformation using dbt
        
        🛠️ **Tools**
        - DuckDB/ClickHouse for storage
        - Prefect for orchestration
        - Databricks for analytics
        
        ☁️ **Infrastructure**
        - GKE for deployment
        - Hybrid cloud setup
        - Auto-scaling enabled
        """)
    # Implementation Notes
    with st.expander("📝 Implementation Notes"):
        st.markdown("""
        **Phase 1: Foundation**
        - Set up DuckDB for initial data storage
        - Implement basic Airbye validation
        - Configure Prefect workflows
        
        **Phase 2: Scaling**
        - Migrate to ClickHouse for larger datasets
        - Integrate Soda for comprehensive monitoring
        - Implement dbt models
        
        **Phase 3: Enterprise**
        - Deploy on GKE
        - Integrate with Databricks
        - Implement full automation
        
        > **Note:** This architecture ensures scalability from thousands to millions of records while maintaining data quality standards.
        """)
    
    # Recommendations Section
    st.markdown("### 📋 Next Steps")
    st.markdown("""
    1. Review the **Data Analysis** tab for detailed insights into each issue
    2. Use the **Data Cleaning** tab to address identified problems
    3. Explore **CPG Queries** tab for industry-specific analysis
    4. Generate a comprehensive report in the **Report** tab
    """)
    
    # Data Privacy Note
    st.markdown("""
    ---
    > **Note:** All analysis is performed locally on your data. No information is sent to external servers.
    """)



def render_data_analysis_tab():
    """Render the Data Analysis tab with data loading and analysis sections."""
    # Check if data has been loaded
    if st.session_state.df is None or st.session_state.table_name is None:
        render_no_data_message("analysis")
        return
    
    # If analysis hasn't been run yet, do it now
    if st.session_state.analysis is None:
        with st.spinner("Analyzing your data..."):
            # Actual analysis
            st.session_state.analysis = analyze_data(st.session_state.df, st.session_state.table_name)
            st.session_state.issues = identify_data_quality_issues(st.session_state.df, st.session_state.analysis, st.session_state.table_name)
        
        # Mark this step as completed
        st.session_state.completed_steps.add('analysis')
        st.session_state.active_step = 2

    # Show a success message if analysis is complete
    if 'analysis' in st.session_state.completed_steps:
        create_info_box("Analysis Complete", "Explore the results below.", "success")
    
    # Display key metrics at the top
    if st.session_state.analysis is not None:
        render_key_metrics(st.session_state.analysis)
    
    # Create a more intuitive tabbed interface with icons
    analysis_tabs = st.tabs(["📊 Data Overview", "📋 Column Analysis"])
    
    # Display data overview and column analysis with spinners
    with st.spinner("Loading data overview..."):
        display_data_overview(analysis_tabs)
    with st.spinner("Analyzing columns..."):
        display_column_analysis(analysis_tabs)


def render_data_cleaning_tab():
    """Render the Data Cleaning tab with recommendations and cleaning options."""
    st.markdown('<h2 class="sub-header">Data Cleaning</h2>', unsafe_allow_html=True)
    
    # Update active step to cleaning (step 3 in our new structure)
    if 'analysis' in st.session_state.completed_steps:
        st.session_state.active_step = 3
    
    # Check if data has been loaded
    if st.session_state.df is None or st.session_state.table_name is None:
        render_no_data_message("cleaning")
        return
    
    if st.session_state.issues is None:
        # Run analysis if not already done
        with st.spinner("Cleaning your data..."):
            st.session_state.analysis = analyze_data(st.session_state.df, st.session_state.table_name)
            st.session_state.issues = identify_data_quality_issues(st.session_state.df, st.session_state.analysis, st.session_state.table_name)
    
    # Generate cleaning recommendations with spinner
    with st.spinner("Generating cleaning recommendations..."):
        recommendations = generate_cleaning_recommendations(st.session_state.df, st.session_state.issues, st.session_state.analysis)
    
    # Create tabs for different cleaning operations
    cleaning_tabs = st.tabs(["Recommendations", "Data Completeness", "Deduplication"])
    
    with cleaning_tabs[0]:
        st.markdown("**Cleaning Recommendations**")
        st.markdown("Based on the analysis, the following cleaning operations are recommended:")
        
        # Display recommendations with spinner
        with st.spinner("Loading recommendations..."):
            for i, rec in enumerate(recommendations):
                with st.expander(f"{i+1}. {rec.get('title', 'Recommendation')}"):
                    st.markdown(f"**Issue**: {rec.get('issue', '')}")
                    st.markdown(f"**Impact**: {rec.get('impact', '')}")
                    
                    st.markdown("**Recommended Actions:**")
                    for action in rec.get('actions', []):
                        st.markdown(f"- {action}")
                    
                    # Add "Apply This Fix" button for each recommendation if applicable
                    if 'auto_fix' in rec and rec['auto_fix']:
                        if st.button("Apply This Fix", key=f"fix_{i}"):
                            cleaning_step = rec.get('cleaning_step', {})
                            if cleaning_step:
                                with st.spinner("Applying fix..."):
                                    cleaned_df = clean_data(st.session_state.df, st.session_state.table_name, [cleaning_step])
                                    if cleaned_df is not None:
                                        st.session_state.df = cleaned_df
                                        st.success("Fix applied successfully!")
                                        # Update analysis after cleaning
                                        st.session_state.analysis = analyze_data(st.session_state.df, st.session_state.table_name)
                                        st.session_state.issues = identify_data_quality_issues(st.session_state.df, st.session_state.analysis, st.session_state.table_name)
                                        st.experimental_rerun()
    
    # Display cleaning options with spinners
    with st.spinner("Loading cleaning options..."):
        display_basic_cleaning_options(cleaning_tabs)
        display_data_completeness_options(cleaning_tabs)
        display_deduplication_options(cleaning_tabs)


def render_report_tab():
    """Render the Report tab with data quality report and export options."""
    st.markdown('<h2 class="sub-header">Data Quality Report</h2>', unsafe_allow_html=True)
    
    # Check if data has been loaded
    if st.session_state.df is None or st.session_state.table_name is None:
        render_no_data_message("report generation")
        return
    
    # Render the data quality report with spinner
    with st.spinner("Generating report..."):
        render_data_quality_report()


def render_cpg_analysis_tab():
    """Render the CPG Analysis tab with specialized CPG queries and visualizations."""
    # Check if data has been loaded
    if st.session_state.df is None or st.session_state.table_name is None:
        render_no_data_message("CPG analysis")
        return
    
    # Tab 2: CPG Analysis Queries with spinner
    with st.spinner("Building queries..."):
        display_cpg_analysis_queries()


def toggle_advanced_options():
    """Toggle the visibility of advanced options."""
    st.session_state.show_advanced_options = not st.session_state.show_advanced_options


def main():
    """Main function to run the Streamlit application."""
    # Setup page configuration and styling
    setup_page()
    
    # Initialize session state variables
    initialize_session_state()
    
    # Display application header
    display_app_header()
    
    # Render welcome section and get data source selection
    selected_table, _, load_clicked = render_welcome_section()
    
    # Handle data loading
    if load_clicked:
        with st.spinner("Loading data from DuckDB..."):
            # Load data from DuckDB
            from util.database import load_data_from_table
            table_name, df = load_data_from_table(selected_table)
            
            if df is not None and not df.empty:
                st.session_state.df = df
                st.session_state.table_name = table_name
                st.success(f"Loaded {len(df)} records from {table_name}")
                # Show workflow progress steps immediately after success message
                create_progress_steps(
                    ["Load Data", "Analyze", "Clean Data","CPG Queries", "Generate Report"],
                    st.session_state.active_step,
                    st.session_state.completed_steps
                )
            else:
                st.error(f"Failed to load data from {selected_table}. Please check the database connection.")
    
    # Create tabs with icons for better visual hierarchy
    tabs = st.tabs(["🏠 Overview", "📊 Data Analysis", "🧹 Data Cleaning", "🛒 CPG Queries", "📝 Report"])
    
    # Tab 1: Overview
    with tabs[0]:
        render_overview()
    
    # Tab 2: Data Analysis
    with tabs[1]:
        render_data_analysis_tab()
    
    # Tab 3: Data Cleaning
    with tabs[2]:
        render_data_cleaning_tab()
    
    # Tab 4: CPG Analysis
    with tabs[3]:
        render_cpg_analysis_tab()
    
    # Tab 5: Report
    with tabs[4]:
        render_report_tab()
    
    # Footer with helpful information
    render_footer()


# Run the application
if __name__ == "__main__":
    main()
