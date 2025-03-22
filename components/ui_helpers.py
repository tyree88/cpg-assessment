"""
UI Helper Components

This module contains small, reusable UI components for the Streamlit interface.
These components help maintain consistency and reduce code duplication.
"""

import streamlit as st
import time
from typing import Dict, Any, Tuple

def render_data_source_selector() -> Tuple[str, bool]:
    """
    Render the data source selection UI component using only DuckDB tables.
    
    Returns:
        Tuple containing the selected table name and load button status
    """
    from util.database import get_tables
    
    # Create columns for data source selection - rearranged to put load button on left
    source_col1, source_col2 = st.columns([1, 3])
    
    with source_col1:
        # Load button with prominent styling
        st.markdown("<p>Click to load the selected data:</p>", unsafe_allow_html=True)
        load_clicked = st.button("📊 Load Data", key="load_data_btn", use_container_width=True)
    
    with source_col2:
        # Get actual tables from DuckDB
        table_options = get_tables()
        
        if not table_options:
            st.warning("No tables found in the DuckDB database. Please create tables first.")
            table_options = ["No tables available"]
            
        selected_table = st.selectbox("Select a database table", table_options)
        
        # Only allow loading if a valid table is selected
        load_clicked = load_clicked and selected_table != "No tables available"
    
    return selected_table, False, load_clicked

def render_no_data_message(component_name: str):
    """
    Display a standardized message when no data is loaded.
    
    Args:
        component_name: Name of the component requesting data
    """
    from components.ui_components import create_info_box
    
    # Display alert using our info_box component
    create_info_box(
        "No Data Loaded", 
        "Please load data first using the Data Selection section at the top of the page.",
        "warning"
    )
    
    # Show a helpful image or animation
    st.markdown(f"""
    <div style="text-align: center; padding: 20px;">
        <img src="https://cdn.pixabay.com/photo/2017/06/10/07/21/folder-2389238_960_720.png" width="150">
        <p>Select a data source to begin {component_name}</p>
    </div>
    """, unsafe_allow_html=True)

def render_key_metrics(analysis: Dict[str, Any]):
    """
    Render key metrics based on analysis results.
    
    Args:
        analysis: Dictionary containing analysis results
    """
    # Display metrics in a cleaner, more streamlined way
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Calculate data quality score
        quality_score = analysis.get('quality_score', 75)
        st.metric("Data Quality Score", f"{quality_score}%")
    
    with col2:
        # Get issue counts
        critical_issues = len(st.session_state.issues.get('critical', []))
        st.metric("Critical Issues", critical_issues)
    
    with col3:
        # Get completeness score
        completeness = analysis.get('completeness', 85)
        st.metric("Data Completeness", f"{completeness}%")

def render_advanced_options():
    """Render advanced analysis options in an expander."""
    with st.expander("🔧 Advanced Analysis Options", expanded=st.session_state.show_advanced_options):
        st.session_state.show_advanced_options = True
        
        st.markdown("<p class='section-header'>Customize Analysis Parameters</p>", unsafe_allow_html=True)
        
        col1, col2 = st.columns(2)
        with col1:
            st.slider("Missing Value Threshold (%)", 0, 100, 20, 
                     help="Set the threshold for flagging columns with missing values")
        with col2:
            st.slider("Duplicate Detection Sensitivity", 1, 10, 5, 
                     help="Higher values detect more potential duplicates")
        
        st.checkbox("Include statistical outlier detection", 
                   help="Uses Z-score to identify outliers in numerical columns")
        st.checkbox("Perform correlation analysis", 
                   help="Analyzes relationships between numerical columns")

def render_footer():
    """Render the application footer with helpful links."""
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; color: #666; padding: 10px; font-size: 0.8rem;">
        <p>DataPlor - CPG Data Quality Assessment Tool</p>
        <p>Need help? Check out the <a href="#">documentation</a> or <a href="#">contact support</a>.</p>
    </div>
    """, unsafe_allow_html=True)

def render_welcome_section():
    st.markdown("<h2 class='section-header'>📋 Getting Started</h2>", unsafe_allow_html=True)
    
    # Warning about known issues
    st.warning("⚠️ **Known Issue**: When you run a query it will take you back to the home tab.. Continue back to the query tab to continue analysis! This is issue still being ironed out")
    
    # Quick start guide prominently displayed
    st.markdown("""
    ### Quick Start Guide:
    1. Select your data source from the dropdown menu below
    2. Click 'Load Data' to begin the analysis
    3. Navigate through the tabs to explore different aspects of your data
    4. Generate a final report with findings and recommendations
    """)
    
    
    # Detailed information in expander
    with st.expander("ℹ️ Learn More About DataPlor Features"):
        st.markdown("""
        DataPlor provides a comprehensive data quality assessment for Consumer Packaged Goods (CPG) point-of-interest and location data. 
        
        ### Key Features:
        
        #### 1. Data Loading & Overview
        - Connect to DuckDB and load your data tables
        - View basic statistics and data summaries
        - Understand data structure and completeness
        
        #### 2. Data Analysis
        - Detailed column-level analysis
        - Data type validation and consistency checks
        - Missing value analysis and pattern detection
        - Duplicate record identification
        
        #### 3. Data Cleaning
        - Automated cleaning recommendations
        - Handle missing data and duplicates
        - Standardize formats and values
        - Track cleaning operations and their impact
        
        #### 4. CPG-Specific Analysis
        - Business category distribution
        - Location clustering analysis
        - Market coverage assessment
        - Competition analysis
        
        #### 5. Report Generation
        - Comprehensive data quality reports
        - Issue summaries and recommendations
        - Export findings and visualizations
        - Action item prioritization
        
        > **Note:** All analysis is performed locally and no data is sent to external servers.
        """)
    # Add data loader element with minimal spacing
    st.markdown("<h4 style='margin-top: 0.5rem; margin-bottom: 0.5rem;'>Select Your Data Source</h4>", unsafe_allow_html=True)
    
        # Render data source selector
    selected_table, use_sample, load_clicked = render_data_source_selector()
    
    return selected_table, use_sample, load_clicked
