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
    
    # Create columns for data source selection
    source_col1, source_col2 = st.columns([3, 1])
    
    with source_col1:
        # Get actual tables from DuckDB
        table_options = get_tables()
        
        if not table_options:
            st.warning("No tables found in the DuckDB database. Please create tables first.")
            table_options = ["No tables available"]
            
        selected_table = st.selectbox("Select a database table", table_options)
    
    with source_col2:
        # Load button with prominent styling
        st.markdown("<p>Click to load the selected data:</p>", unsafe_allow_html=True)
        load_clicked = st.button("📊 Load Data", key="load_data_btn", use_container_width=True)
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

def render_analysis_progress(progress_container):
    """
    Render a progress bar for data analysis.
    
    Args:
        progress_container: Streamlit container to render progress in
    """
    with progress_container:
        progress_bar = st.progress(0)
        with st.spinner("Analyzing your data..."):
            # Simulate progress for better UX
            for i in range(100):
                # Update progress bar
                progress_bar.progress(i + 1)
                time.sleep(0.01)

def render_key_metrics(analysis: Dict[str, Any]):
    """
    Render key metrics cards based on analysis results.
    
    Args:
        analysis: Dictionary containing analysis results
    """
    from components.ui_components import create_metric_card
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Calculate data quality score
        quality_score = analysis.get('quality_score', 75)
        create_metric_card(
            "Data Quality Score", 
            f"{quality_score}%", 
            "Overall quality assessment", 
            "📊",
            quality_score >= 70
        )
    
    with col2:
        # Get issue counts
        critical_issues = len(st.session_state.issues.get('critical', []))
        create_metric_card(
            "Critical Issues", 
            critical_issues, 
            "Issues requiring immediate attention", 
            "⚠️",
            critical_issues == 0
        )
    
    with col3:
        # Get completeness score
        completeness = analysis.get('completeness', 85)
        create_metric_card(
            "Data Completeness", 
            f"{completeness}%", 
            "Percentage of non-null values", 
            "✓",
            completeness >= 80
        )

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
    """Render the welcome section with getting started information."""
    from components.ui_components import create_info_box
    
    st.markdown("<h3 class='section-header'>📋 Getting Started</h3>", unsafe_allow_html=True)
    create_info_box(
        "Welcome to DataPlor!", 
        """
        This tool helps you assess and improve data quality for CPG data.
        Follow these simple steps:
        1. Select your data source below
        2. Explore the analysis in the tabs
        3. Generate recommendations and reports
        """,
        "info"
    )
    
    # Add data loader element below the welcome message
    st.markdown("<h4 style='margin-top: 1rem;'>Select Your Data Source</h4>", unsafe_allow_html=True)
    st.markdown("""<div class="metric-card">""", unsafe_allow_html=True)
    
    # Render data source selector
    selected_table, use_sample, load_clicked = render_data_source_selector()
    
    st.markdown("</div>", unsafe_allow_html=True)
    
    return selected_table, use_sample, load_clicked
