import streamlit as st
import pandas as pd
from components.ui_helpers import render_advanced_options
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

def display_quality_issues(analysis_tabs):
    """Display data quality issues categorized by severity."""
    with analysis_tabs[1]:
        st.subheader("Quality Issues")
        
        # Display critical issues
        if st.session_state.issues.get('critical', []):
            st.markdown('<div style="border-left: 4px solid #D32F2F; padding-left: 12px;">', unsafe_allow_html=True)
            st.markdown('<p class="critical-issue"><strong>Critical Issues</strong></p>', unsafe_allow_html=True)
            for issue in st.session_state.issues.get('critical', []):
                st.markdown(f"**{issue.get('type', '')}**: {issue.get('description', '')}")
            st.markdown('</div>', unsafe_allow_html=True)
        
        # Display warnings
        if st.session_state.issues.get('warnings', []):
            st.markdown('<div style="border-left: 4px solid #FF9800; padding-left: 12px;">', unsafe_allow_html=True)
            st.markdown('<p class="warning-issue"><strong>Warnings</strong></p>', unsafe_allow_html=True)
            for issue in st.session_state.issues.get('warnings', []):
                st.markdown(f"**{issue.get('type', '')}**: {issue.get('description', '')}")
            st.markdown('</div>', unsafe_allow_html=True)
        
        # Display info issues
        if st.session_state.issues.get('info', []):
            st.markdown('<div style="border-left: 4px solid #1976D2; padding-left: 12px;">', unsafe_allow_html=True)
            st.markdown('<p class="info-text"><strong>Information</strong></p>', unsafe_allow_html=True)
            for issue in st.session_state.issues.get('info', []):
                st.markdown(f"**{issue.get('type', '')}**: {issue.get('description', '')}")
            st.markdown('</div>', unsafe_allow_html=True)
        
        # Display quality score if available
        if 'quality_score' in st.session_state.issues:
            st.subheader("Data Quality Score")
            score = st.session_state.issues['quality_score']
            st.progress(score / 100)
            col1, col2 = st.columns([1, 3])
            with col1:
                st.metric("Quality Score", score)
            with col2:
                if score < 50:
                    st.markdown('<span class="critical-issue">Poor quality - significant issues need to be addressed</span>', unsafe_allow_html=True)
                elif score < 75:
                    st.markdown('<span class="warning-issue">Fair quality - some issues need attention</span>', unsafe_allow_html=True)
                else:
                    st.markdown('<span class="good-quality">Good quality - minor improvements possible</span>', unsafe_allow_html=True)

def display_column_analysis(analysis_tabs):
    """Display detailed analysis for a selected column."""
    with analysis_tabs[1]:
        st.subheader("Column Analysis")
        
        # Allow user to select a column for detailed analysis
        selected_column = st.selectbox("Select a column for detailed analysis:", st.session_state.df.columns)
        
        if selected_column:
            col1, col2 = st.columns(2)
            
            with col1:
                # Column statistics
                st.markdown('<div style="padding: 15px; background-color: #f5f5f5; border-radius: 5px;">', unsafe_allow_html=True)
                st.markdown(f"**Column Statistics: {selected_column}**")
                
                # Add advanced options expander here
                render_advanced_options()
                
                # Get column data
                col_data = st.session_state.df[selected_column]
                missing_info = st.session_state.analysis['missing_values'][selected_column]
                
                # Display basic stats
                st.markdown(f"**Data Type**: {st.session_state.analysis['column_types'][selected_column]}")
                st.markdown(f"**Missing Values**: {missing_info['count']} ({missing_info['percent']}%)")
                
                # Display numeric stats if applicable
                if pd.api.types.is_numeric_dtype(col_data.dtype):
                    st.markdown(f"**Min**: {col_data.min()}")
                    st.markdown(f"**Max**: {col_data.max()}")
                    st.markdown(f"**Mean**: {col_data.mean():.2f}")
                    st.markdown(f"**Median**: {col_data.median()}")
                else:
                    # Count unique values
                    unique_count = col_data.nunique()
                    st.markdown(f"**Unique Values**: {unique_count}")
                st.markdown('</div>', unsafe_allow_html=True)
            
            with col2:
                # Display sample values
                st.markdown('<div style="padding: 15px; background-color: #f5f5f5; border-radius: 5px;">', unsafe_allow_html=True)
                st.markdown(f"**Sample Values: {selected_column}**")
                sample_values = col_data.dropna().sample(min(5, len(col_data.dropna()))).tolist()
                for val in sample_values:
                    st.markdown(f"- {val}")
                st.markdown('</div>', unsafe_allow_html=True)
            
            # Display value distribution
            st.subheader("Value Distribution")
            
            # For categorical or low-cardinality columns
            if not pd.api.types.is_numeric_dtype(col_data.dtype) or col_data.nunique() < 15:
                # Get value counts
                value_counts = col_data.value_counts().head(10)
                st.bar_chart(value_counts)
