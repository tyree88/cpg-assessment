import streamlit as st
# Import utility modules
from util.analysis import analyze_data
from util.cleaning import identify_data_quality_issues, generate_cleaning_recommendations, clean_data

# Import components
from components.data_loader import load_data_section
from components.data_analysis import display_column_analysis, display_quality_issues, display_data_overview
from components.data_cleaning import display_data_completeness_options, display_deduplication_options, display_basic_cleaning_options
from components.cpg_metrics import render_cpg_metrics_tabs
from components.data_report import render_data_quality_report

def setup_page():
    """Configure the Streamlit page settings and apply custom CSS."""
    st.set_page_config(page_title="DataPlor - CPG Data Quality Assessment", layout="wide")
    
    # Custom CSS for better styling
    st.markdown("""
    <style>
        .main-header {font-size: 2.5rem !important; color: #1E88E5; font-weight: 600;}
        .sub-header {font-size: 1.5rem !important; color: #424242; margin-bottom: 1rem;}
        .metric-card {background-color: #f5f5f5; border-radius: 5px; padding: 15px; margin: 10px 0;}
        .critical-issue {color: #D32F2F;}
        .warning-issue {color: #FF9800;}
        .good-quality {color: #4CAF50;}
        .info-text {color: #1976D2;}
        .stTabs [data-baseweb="tab-list"] {gap: 24px;}
        .stTabs [data-baseweb="tab"] {height: 50px; white-space: pre-wrap;}
        .stTabs [aria-selected="true"] {background-color: #f0f2f6;}
        .stExpander {border: 1px solid #f0f2f6;}
    </style>
    """, unsafe_allow_html=True)


def initialize_session_state():
    """Initialize session state variables if they don't exist."""
    if 'table_name' not in st.session_state:
        st.session_state.table_name = None
    if 'df' not in st.session_state:
        st.session_state.df = None
    if 'analysis' not in st.session_state:
        st.session_state.analysis = None
    if 'issues' not in st.session_state:
        st.session_state.issues = None


def display_app_header():
    """Display the application header and title."""
    st.markdown('<h1 class="main-header">DataPlor - CPG Data Quality Assessment</h1>', unsafe_allow_html=True)





def render_overview():
    """Render the main overview page with key findings and getting started information."""
    st.markdown('<h2 class="sub-header">CPG Data Quality Assessment Overview</h2>', unsafe_allow_html=True)
    
    st.markdown("""
    This application provides a comprehensive data quality assessment for CPG Client's point-of-interest (POI) 
    and location data. The assessment identifies data quality issues and provides actionable recommendations for improvement.
    """)
    
    # Key Findings Section
    st.markdown("### Key Findings")
    st.markdown("Our analysis of 37,790 location records revealed several critical data quality issues:")
    
    col1, col2 = st.columns(2)
    
    with col1:
        with st.container():
            st.markdown('<p class="critical-issue"><strong>Data Completeness Gaps:</strong></p>', unsafe_allow_html=True)
            st.markdown("""
            - 9.3% of records missing address data
            - 27.2% missing website information
            - Up to 76.7% missing operational hours (particularly on weekends)
            """)
            
        with st.container():
            st.markdown('<p class="warning-issue"><strong>Duplicate Records:</strong></p>', unsafe_allow_html=True)
            st.markdown("""
            - 132 potential duplicate business locations identified
            - Examples include "Silvercreek Realty Group" (5 duplicates) and "Americana Terrace" (4 duplicates)
            """)
    
    with col2:
        with st.container():
            st.markdown('<p class="warning-issue"><strong>Data Confidence Issues:</strong></p>', unsafe_allow_html=True)
            st.markdown("""
            - 37.3% of records have low confidence scores (below 0.7)
            - Average confidence score across all records: 0.745
            """)
            
        with st.container():
            st.markdown('<p class="critical-issue"><strong>Format Standardization Issues:</strong></p>', unsafe_allow_html=True)
            st.markdown("""
            - Multiple postal code formats (5-digit, 9-digit, and non-standard)
            - Character encoding problems in some business names
            """)
    
    # This section has been moved to below the header


def render_data_analysis_tab():
    """Render the Data Analysis tab with data loading and analysis sections."""
    st.markdown('<h2 class="sub-header">Data Analysis</h2>', unsafe_allow_html=True)
    
    # Data loading is now handled at the top of the page
    
    # Check if data has been loaded
    if st.session_state.df is None or st.session_state.table_name is None:
        # Display alert if no data is loaded
        st.info("Please load data in the Data Analysis tab first.")
    else:
        if st.session_state.analysis is None:
            # Run analysis
            with st.spinner("Analyzing data..."):
                st.session_state.analysis = analyze_data(st.session_state.df, st.session_state.table_name)
                st.session_state.issues = identify_data_quality_issues(st.session_state.df, st.session_state.analysis, st.session_state.table_name)
        
        # Create tabs for different analysis sections
        analysis_tabs = st.tabs(["Data Overview", "Quality Issues", "Column Analysis"])
        
        # Display data overview
        display_data_overview(analysis_tabs)
        

        
        # Display quality issues
        display_quality_issues(analysis_tabs)
        
        # Display column analysis
        display_column_analysis(analysis_tabs)
        
def render_data_cleaning_tab():
    """Render the Data Cleaning tab with recommendations and cleaning options."""
    st.markdown('<h2 class="sub-header">Data Cleaning</h2>', unsafe_allow_html=True)
    
    if st.session_state.df is not None and st.session_state.table_name is not None:
        if st.session_state.issues is None:
            # Run analysis if not already done
            with st.spinner("Analyzing data..."):
                st.session_state.analysis = analyze_data(st.session_state.df, st.session_state.table_name)
                st.session_state.issues = identify_data_quality_issues(st.session_state.df, st.session_state.analysis, st.session_state.table_name)
        
        # Generate cleaning recommendations
        recommendations = generate_cleaning_recommendations(st.session_state.df, st.session_state.issues, st.session_state.analysis)
        
        # Create tabs for different cleaning operations
        cleaning_tabs = st.tabs(["Recommendations", "Data Completeness", "Deduplication"])
        
        with cleaning_tabs[0]:
            st.markdown("**Cleaning Recommendations**")
            st.markdown("Based on the analysis, the following cleaning operations are recommended:")
            
            # Display recommendations
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
        
        display_basic_cleaning_options(cleaning_tabs)
        display_data_completeness_options(cleaning_tabs)
        display_deduplication_options(cleaning_tabs)
    else:
        st.info("Please load data in the Data Analysis tab first.")


def render_report_tab():
    """Render the Report tab with data quality report and export options."""
    st.markdown('<h2 class="sub-header">Data Quality Report</h2>', unsafe_allow_html=True)
    
    # Check if data has been loaded
    if st.session_state.df is None or st.session_state.table_name is None:
        # Display alert if no data is loaded
        st.info("Please load data in the Data Analysis tab first.")
    else:
        # Render the data quality report
        render_data_quality_report()


def render_cpg_analysis_tab():
    """Render the CPG Analysis tab with specialized CPG queries and visualizations."""
    st.markdown('<h2 class="sub-header">CPG Analysis</h2>', unsafe_allow_html=True)
    
    # Check if data has been loaded
    if st.session_state.df is None or st.session_state.table_name is None:
        # Display alert if no data is loaded
        st.info("Please load data in the Data Analysis tab first.")
    else:
        # Create tabs for CPG metrics and queries
        cpg_tabs = st.tabs(["Data Quality Metrics", "Analysis Queries"])
        
        # Tab 1: CPG Data Quality Metrics
        with cpg_tabs[0]:
            render_cpg_metrics_tabs()
            
        # Tab 2: CPG Analysis Queries
        with cpg_tabs[1]:
            from components.cpg_queries import display_cpg_analysis_queries
            display_cpg_analysis_queries()


def main():
    """Main function to run the Streamlit application."""
    # Setup page configuration and styling
    setup_page()
    
    # Initialize session state variables
    initialize_session_state()
    
    # Display application header
    display_app_header()
    
    # Getting Started Section - Condensed
    st.markdown("### Getting Started")
    st.info("""
    To begin analyzing your data, use the Data Selection section below to load your data from the database 
    or upload a new file. Then explore the **CPG Analysis** tab to view detailed metrics.
    """)
    
    # Add data loader component below the header
    st.markdown('<p class="sub-header">Data Selection</p>', unsafe_allow_html=True)
    load_data_section(section_key="global")
    
    # Add instructions on how to load data
    st.markdown("""<div style='padding: 10px; background-color: #000000; border-radius: 5px; color: white; margin-top: 10px;'>
        <h3 style='color: white;'>How to load data:</h3>
        <ol style='color: white;'>
            <li>Select a table from the dropdown menu above</li>
            <li>Click the <b>Load Data</b> button</li>
        </ol>
        <p style='color: white;'>Once data is loaded, you can explore the analysis in the tabs below.</p>
    </div>""", unsafe_allow_html=True)
    
    # Create tabs for different sections of the application
    tabs = st.tabs(["Overview", "Data Analysis", "Data Cleaning", "CPG Analysis", "Report"])
    
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


# Run the application
if __name__ == "__main__":
    main()
