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
from util.cleaning import identify_data_quality_issues, generate_cleaning_recommendations
from util.styles import apply_all_styles

# Import components
from components.data_analysis import display_data_overview
from components.data_report import render_data_quality_report
from components.ui_components import create_progress_steps, create_info_box
from components.cpg_queries import display_cpg_analysis_queries
from components.overview import render_overview
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


def render_data_analysis_tab():
    """Render the Data Analysis tab with data loading and analysis sections."""
    # Check if data has been loaded
    if st.session_state.get('df') is None or st.session_state.get('table_name') is None:
        st.warning("Please load data first.")
        return

    # Check if analysis has been run
    if st.session_state.get('analysis') is None:
        st.warning("Analysis has not been run yet. Please run the analysis first.")
        return

    st.header("Data Analysis")
    
    # Create analysis tabs - only Data Overview
    analysis_tabs = st.tabs(["Data Overview"])
    
    # Display data overview
    with st.spinner("Loading data overview..."):
        display_data_overview(analysis_tabs)


def render_data_cleaning_tab():
    """Render the Data Cleaning tab with recommendations."""    
    # Check if data has been loaded
    if st.session_state.df is None or st.session_state.table_name is None:
        render_no_data_message("cleaning")
        return
    
    if st.session_state.issues is None:
        # Run analysis if not already done
        with st.spinner("Analyzing data quality..."):
            st.session_state.analysis = analyze_data(st.session_state.df, st.session_state.table_name)
            st.session_state.issues = identify_data_quality_issues(st.session_state.df, st.session_state.analysis, st.session_state.table_name)
    
    # Generate cleaning recommendations with spinner
    with st.spinner("Generating cleaning recommendations..."):
        recommendations = generate_cleaning_recommendations(st.session_state.df, st.session_state.issues, st.session_state.analysis)
    
    st.markdown("### Data Quality Recommendations")
    st.markdown("Based on the analysis, here are the recommended data quality improvements:")
    
    # Display recommendations with spinner
    with st.spinner("Loading recommendations..."):
        for i, rec in enumerate(recommendations):
            with st.expander(f"**Issue**: {rec.get('issue', '')}"):
                
                st.markdown(f"**Impact**: {rec.get('impact', '')}")
                
                st.markdown("**Recommended Actions:**")
                for action in rec.get('actions', []):
                    st.markdown(f"- {action}")


def render_report_tab():
    """Render the Report tab with data quality report and export options."""
    
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
                # Update session state with new data
                st.session_state.df = df
                st.session_state.table_name = table_name
                st.session_state.analysis = None  # Reset analysis
                st.session_state.issues = None  # Reset issues
                st.success(f"Loaded {len(df)} records from {table_name}")
                
                # Run analysis immediately after loading data
                with st.spinner("Analyzing your data.... Cleaning your data.... Building queries.... Generating report...."):
                    # Actual analysis
                    st.session_state.analysis = analyze_data(st.session_state.df, st.session_state.table_name)
                    st.session_state.issues = identify_data_quality_issues(st.session_state.df, st.session_state.analysis, st.session_state.table_name)
                
                # Mark this step as completed
                st.session_state.completed_steps.add('analysis')
                st.session_state.active_step = 2
                
                # Show workflow progress steps immediately after analysis
                create_progress_steps(
                    ["Load Data", "Analyze", "Clean Data", "CPG Queries", "Generate Report"],
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
