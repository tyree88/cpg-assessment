import streamlit as st
import pandas as pd
from typing import Dict, Any, Optional, Tuple, List
import time

from util.database_improved import get_tables, load_data_from_table

# Constants for data loading settings
DEFAULT_SAMPLE_SIZE = 10000  # Default sample size for large tables
MAX_PREVIEW_ROWS = 5  # Maximum rows to show in preview

def initialize_data_loader_state():
    """Initialize session state variables for data loader if they don't exist."""
    if 'data_loading_error' not in st.session_state:
        st.session_state.data_loading_error = None
    if 'loading_time' not in st.session_state:
        st.session_state.loading_time = None
    if 'data_preview' not in st.session_state:
        st.session_state.data_preview = None
    if 'available_tables' not in st.session_state:
        st.session_state.available_tables = None
    if 'last_refresh' not in st.session_state:
        st.session_state.last_refresh = None
    if 'validation_results' not in st.session_state:
        st.session_state.validation_results = None


def refresh_tables() -> List[Dict[str, Any]]:
    """Refresh the list of available tables."""
    tables = get_tables()
    st.session_state.available_tables = tables
    st.session_state.last_refresh = time.time()
    return tables


def load_table_data(table_name: str, use_sample: bool = False, sample_size: Optional[int] = None) -> Tuple[Optional[str], Optional[pd.DataFrame], Optional[Dict[str, Any]]]:
    """Load data from the selected table with error handling and timing.
    
    Args:
        table_name: Name of the table to load
        use_sample: Whether to use sampling for large tables
        sample_size: Sample size for large tables
        
    Returns:
        Tuple of (table_name, dataframe, validation_results) or (None, None, None) if error
    """
    # Reset any previous errors
    st.session_state.data_loading_error = None
    
    try:
        start_time = time.time()
        
        # Determine sample size
        actual_sample_size = None
        if use_sample:
            actual_sample_size = sample_size if sample_size else DEFAULT_SAMPLE_SIZE
            
        # Load the data
        print(f"Loading data from table: {table_name} (sample: {use_sample}, size: {actual_sample_size})")
        table_name, df, validation_results = load_data_from_table(
            table_name, 
            validate=True,
            sample_size=actual_sample_size
        )
        
        end_time = time.time()
        st.session_state.loading_time = round(end_time - start_time, 2)
        
        if df is not None:
            # Create a preview of the data
            st.session_state.data_preview = df.head(MAX_PREVIEW_ROWS)
            st.session_state.validation_results = validation_results
            
            print(f"Successfully loaded data from {table_name}: {len(df)} rows, {len(df.columns)} columns")
            return table_name, df, validation_results
        else:
            st.session_state.data_loading_error = f"Failed to load data from {table_name}."
            print(f"Error: Failed to load data from {table_name}")
            return None, None, None
            
    except Exception as e:
        error_msg = f"Error loading table {table_name}: {str(e)}"
        st.session_state.data_loading_error = error_msg
        print(f"Error: {error_msg}")
        return None, None, None


def display_table_selection(section_key: str = "default", on_change: Optional[callable] = None):
    """Display the table selection dropdown with metadata.
    
    Args:
        section_key: Unique key for this section
        on_change: Optional callback when selection changes
    """
    # Get available tables or use cached ones
    tables = st.session_state.available_tables
    if tables is None or len(tables) == 0 or st.session_state.last_refresh is None or (time.time() - st.session_state.last_refresh) > 300:  # Refresh after 5 minutes
        tables = refresh_tables()
    
    if not tables:
        st.info("No tables available. Please check your database connection.")
        if st.button("Refresh Tables", key=f"refresh_tables_{section_key}"):
            with st.spinner("Refreshing available tables..."):
                tables = refresh_tables()
                if not tables:
                    st.error("No tables found in the database.")
        return None
    
    # Create a formatted display of tables with metadata
    table_options = {}
    for table in tables:
        # Format display with metadata
        row_count = table.get('row_count', 'Unknown')
        col_count = table.get('column_count', 'Unknown')
        display = f"{table['name']} ({row_count} rows, {col_count} cols)"
        table_options[display] = table['name']
    
    # Convert to list for the selectbox
    table_displays = list(table_options.keys())
    
    # Display the dropdown
    selected_display = st.selectbox(
        "Select a table:", 
        table_displays,
        key=f"table_select_display_{section_key}",
        on_change=on_change if on_change else None
    )
    
    # Get the actual table name from the display
    if selected_display:
        return table_options[selected_display]
    return None


def display_loading_options(section_key: str = "default"):
    """Display options for data loading.
    
    Args:
        section_key: Unique key for this section
    """
    with st.expander("Loading Options", expanded=False):
        use_sample = st.checkbox(
            "Use sampling for large tables", 
            value=True,
            key=f"use_sample_{section_key}",
            help="Load only a sample of rows for large tables to improve performance"
        )
        
        sample_size = None
        if use_sample:
            sample_size = st.number_input(
                "Sample size", 
                min_value=100, 
                max_value=100000, 
                value=DEFAULT_SAMPLE_SIZE,
                step=1000,
                key=f"sample_size_{section_key}",
                help="Number of random rows to load from the table"
            )
        
        return {
            'use_sample': use_sample,
            'sample_size': sample_size
        }


def display_data_validation_results():
    """Display data validation results after loading."""
    if st.session_state.validation_results:
        results = st.session_state.validation_results
        
        st.subheader("Data Validation Results")
        
        # Display basic metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Rows", results.get('row_count', 0))
        with col2:
            st.metric("Columns", results.get('column_count', 0))
        with col3:
            st.metric("Missing Values", f"{results.get('null_percentage', 0)}%")
        
        # Display potential issues
        issues = results.get('potential_issues', [])
        if issues:
            st.warning("Potential Data Quality Issues Detected")
            for issue in issues:
                st.write(f"- {issue}")
        else:
            st.success("No significant data quality issues detected")
        
        # Display missing values by column
        if results.get('missing_values'):
            # Find columns with highest missing percentages
            missing_values = results['missing_values']
            problem_columns = [
                (col, info['percent']) 
                for col, info in missing_values.items() 
                if info['percent'] > 5  # Show only columns with >5% missing
            ]
            
            if problem_columns:
                # Sort by missing percentage
                problem_columns.sort(key=lambda x: x[1], reverse=True)
                
                st.subheader("Columns with Missing Values")
                missing_df = pd.DataFrame(
                    problem_columns, 
                    columns=["Column", "Missing %"]
                )
                st.dataframe(missing_df)
            
        # Display duplicates info
        if 'duplicates' in results and results['duplicates']['count'] > 0:
            st.subheader("Duplicate Rows")
            st.write(f"Found {results['duplicates']['count']} duplicate rows ({results['duplicates']['percent']}% of data)")


def load_data_section(section_key: str = "default"):
    """Enhanced data loading section with validation and preview.
    
    Args:
        section_key: A unique key to differentiate this instance from others
    """
    # Initialize session state
    initialize_data_loader_state()
    
    # Create a two-column layout
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Display table selection
        selected_table = display_table_selection(section_key)
        
        # Display loading options
        loading_options = display_loading_options(section_key)
        
        # Add load button
        if selected_table:
            if st.button("Load Data", key=f"load_table_{section_key}", type="primary"):
                with st.spinner("Loading data..."):
                    table_name, df, validation_results = load_table_data(
                        selected_table,
                        use_sample=loading_options['use_sample'],
                        sample_size=loading_options['sample_size']
                    )
                    
                    if df is not None:
                        st.session_state.table_name = table_name
                        st.session_state.df = df
                        st.success(f"Data loaded successfully from '{table_name}'!")
                        
                        # Reset analysis results since we loaded new data
                        st.session_state.analysis = None
                        st.session_state.issues = None
    
    with col2:
        # Show quick stats or info about selected table
        if selected_table and st.session_state.available_tables:
            # Find the selected table metadata
            table_info = next((t for t in st.session_state.available_tables if t['name'] == selected_table), None)
            
            if table_info:
                st.subheader("Table Information")
                st.markdown(f"""
                - **Table:** {table_info['name']}
                - **Rows:** {table_info.get('row_count', 'Unknown')}
                - **Columns:** {table_info.get('column_count', 'Unknown')}
                """)
    
    # Display error message if any
    if st.session_state.data_loading_error:
        st.error(st.session_state.data_loading_error)
    
    # Display data preview if available
    if st.session_state.data_preview is not None:
        st.subheader("Data Preview")
        st.dataframe(st.session_state.data_preview, use_container_width=True)
        
        if st.session_state.loading_time:
            st.caption(f"Data loaded in {st.session_state.loading_time} seconds")
    
    # Display validation results if available
    if st.session_state.validation_results:
        display_data_validation_results()
