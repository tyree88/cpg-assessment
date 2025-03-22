import streamlit as st
import pandas as pd

# Import utility modules
from util.cleaning import generate_cleaning_recommendations

def render_data_quality_report():
    """Render the Data Quality Report with export options."""
    
    if st.session_state.df is not None and st.session_state.table_name is not None and st.session_state.analysis is not None:
        # Generate a comprehensive report
        st.markdown("## CPG Data Quality Assessment Report")
        st.markdown(f"**Dataset: {st.session_state.table_name}**")
        st.markdown(f"**Report Date: {pd.Timestamp.now().strftime('%Y-%m-%d')}**")
        
        # Executive Summary
        st.markdown("### Executive Summary")
        
        # Calculate quality metrics
        completeness = 100 - st.session_state.analysis['overall_missing_percent']
        duplicate_percent = st.session_state.analysis.get('duplicate_rows', {}).get('percent', 0)
        uniqueness = 100 - duplicate_percent
        
        # Quality score
        quality_score = st.session_state.issues.get('quality_score', 0)
        
        st.markdown(f"This report provides a comprehensive assessment of the data quality for the **{st.session_state.table_name}** dataset.")
        st.markdown(f"The dataset contains **{st.session_state.analysis['row_count']}** records with **{st.session_state.analysis['column_count']}** attributes.")
        
        # Quality metrics visualization
        st.markdown("#### Data Quality Metrics")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Completeness", f"{completeness:.1f}%")
        with col2:
            st.metric("Uniqueness", f"{uniqueness:.1f}%")
        with col3:
            st.metric("Overall Quality", f"{quality_score}")
        
        # Key Findings
        st.markdown("### Key Findings")
        
        # Critical issues
        if st.session_state.issues.get('critical', []):
            st.markdown("#### Critical Issues")
            for issue in st.session_state.issues.get('critical', []):
                st.markdown(f"- **{issue.get('type', '')}**: {issue.get('description', '')}")
        
        # Warnings
        if st.session_state.issues.get('warnings', []):
            st.markdown("#### Warnings")
            for issue in st.session_state.issues.get('warnings', []):
                st.markdown(f"- **{issue.get('type', '')}**: {issue.get('description', '')}")
        
        # Recommendations
        recommendations = generate_cleaning_recommendations(st.session_state.df, st.session_state.issues, st.session_state.table_name)
        
        for rec in recommendations:
            st.markdown(f"**{rec.get('issue', 'Recommendation')}**: {rec.get('description', '')}")
            st.markdown("Actions:")
            for action in rec.get('actions', []):
                st.markdown(f"- {action}")
        
        # Export options
        st.markdown("### Export Report")
        if st.button("Generate PDF Report"):
            st.info("PDF report generation would be implemented here in a production environment.")
        
        if st.button("Export Findings as CSV"):
            st.info("CSV export would be implemented here in a production environment.")
    else:
        st.info("Please load and analyze data first to generate a report.")
