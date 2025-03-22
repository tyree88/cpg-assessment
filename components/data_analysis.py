import streamlit as st
import pandas as pd
from components.ui_helpers import render_advanced_options
import matplotlib.pyplot as plt

def display_data_overview(analysis_tabs):
    """Display data overview including basic statistics and sample data."""
    with analysis_tabs[0]:
        st.subheader("Data Overview")
        
        # Display basic statistics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Rows", st.session_state.analysis['row_count'])
        with col2:
            st.metric("Columns", st.session_state.analysis['column_count'])
        with col3:
            st.metric("Missing Data", f"{st.session_state.analysis['overall_missing_percent']}%")
        with col4:
            duplicate_percent = st.session_state.analysis.get('duplicate_rows', {}).get('percent', 0)
            st.metric("Duplicates", f"{duplicate_percent}%")
        
        # Display sample data
        st.subheader("Sample Data")
        st.dataframe(st.session_state.df.head(10), use_container_width=True)
        
        # Display column information
        st.subheader("Column Information")
        
        # Create a DataFrame for column info
        column_info = []
        for col in st.session_state.df.columns:
            missing_percent = st.session_state.analysis['missing_values'][col]['percent']
            column_info.append({
                "Column": col,
                "Type": st.session_state.analysis['column_types'][col],
                "Missing": f"{missing_percent}%",
                "Sample Values": ", ".join(st.session_state.df[col].dropna().astype(str).head(3).tolist())
            })
        
        st.dataframe(pd.DataFrame(column_info), use_container_width=True)
