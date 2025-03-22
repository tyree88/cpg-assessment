"""
Core UI components for data analysis display.
Simplified to essential visualizations.
"""
import streamlit as st
import pandas as pd
from typing import Dict, Any

def display_data_summary(df: pd.DataFrame, analysis: Dict[str, Any]):
    """Display key data metrics in a simple layout."""
    st.header("Data Summary")
    
    # Basic metrics
    cols = st.columns(3)
    with cols[0]:
        st.metric("Total Records", analysis['basic_stats']['row_count'])
    with cols[1]:
        st.metric("Missing Data", f"{analysis['basic_stats']['missing_percent']:.1f}%")
    with cols[2]:
        st.metric("Duplicates", analysis['quality_metrics']['duplicates'])
    
    # Sample data
    st.subheader("Sample Data")
    st.dataframe(df.head(5))

def display_quality_issues(analysis: Dict[str, Any]):
    """Show data quality issues in a simple format."""
    st.header("Quality Issues")
    
    # Missing data by column
    st.subheader("Missing Data by Column")
    missing_df = pd.DataFrame([
        {
            'Column': col,
            'Missing %': info['percent']
        }
        for col, info in analysis['quality_metrics']['missing_by_column'].items()
        if info['percent'] > 0
    ])
    if not missing_df.empty:
        st.dataframe(missing_df.sort_values('Missing %', ascending=False))
        
        # Highlight critical issues
        critical_missing = missing_df[missing_df['Missing %'] > 20]
        if not critical_missing.empty:
            st.warning("Critical Issues: The following columns have more than 20% missing data:")
            for _, row in critical_missing.iterrows():
                st.write(f"- {row['Column']}: {row['Missing %']:.1f}% missing")

def display_location_analysis(analysis: Dict[str, Any]):
    """Display location data analysis if available."""
    if analysis.get('location_stats'):
        st.header("Location Data")
        valid_coords = analysis['location_stats']['valid_coords']
        total_records = analysis['basic_stats']['row_count']
        valid_percent = (valid_coords / total_records) * 100
        
        st.metric(
            "Valid Coordinates",
            f"{valid_coords:,} ({valid_percent:.1f}%)"
        )
        
        if valid_percent < 90:
            st.warning(
                "Warning: Less than 90% of records have valid coordinates. "
                "This may impact location-based analysis."
            ) 