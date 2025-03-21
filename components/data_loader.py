import streamlit as st
from util.database import get_tables, load_data_from_table

def load_data_section(section_key="default"):
    """Handle data loading from existing tables or file uploads.
    
    Args:
        section_key (str): A unique key to differentiate this instance from others
    """
    # Simplified UI - only show table selection
    
    # Get available tables
    tables = get_tables()
    
    if tables:
        selected_table = st.selectbox("Select a table:", tables, key=f"table_select_{section_key}")
        
        if st.button("Load Data", key=f"load_table_{section_key}"):
            with st.spinner("Loading data..."):
                st.session_state.table_name, st.session_state.df = load_data_from_table(selected_table)
                st.success(f"Data loaded from table '{selected_table}'!")
                # Reset analysis results
                st.session_state.analysis = None
                st.session_state.issues = None
                
                # Data has been loaded successfully
                # Cleaning functionality has been moved to the Data Cleaning tab
                
                # Preview tables have been removed as requested
    else:
        st.info("No tables available.")
