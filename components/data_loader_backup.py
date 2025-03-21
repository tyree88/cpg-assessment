import streamlit as st
import os
import tempfile
from util.database import get_tables, load_data_from_table, load_data_from_file, save_cleaned_data
from util.cleaning import clean_data

def load_data_section(section_key="default"):
    """Handle data loading from existing tables or file uploads.
    
    Args:
        section_key (str): A unique key to differentiate this instance from others
    """
    # Data source selection
    data_source = st.radio("Select data source:", ["Existing Table", "Upload File"], horizontal=True, key=f"data_source_{section_key}")
    
    if data_source == "Existing Table":
        st.markdown('<div style="padding: 15px; background-color: #f5f5f5; border-radius: 5px;">', unsafe_allow_html=True)
        st.markdown("**Load from existing table**")
        
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
                    
                    # Display the loaded table
                    if st.session_state.df is not None:
                        st.subheader(f"Preview of '{selected_table}'")
                        st.dataframe(st.session_state.df.head(10), use_container_width=True)
                        
                        # Show data cleaning in progress
                        with st.spinner("Cleaning data..."):
                            # Define basic cleaning steps
                            cleaning_steps = [
                                {"type": "fill_missing", "column": "address", "value": "Unknown"}, 
                                {"type": "remove_duplicates"}
                            ]
                            
                            # Clean the data
                            cleaned_df, changes = clean_data(st.session_state.df, selected_table, cleaning_steps)
                            
                            # Save cleaned data to database
                            cleaned_table_name = save_cleaned_data(cleaned_df, selected_table)
                            
                            if cleaned_table_name:
                                st.session_state.cleaned_table_name = cleaned_table_name
                                st.session_state.cleaned_df = cleaned_df
                                
                                # Display cleaning summary
                                st.success("Data cleaning completed successfully!")
                                
                                # Show changes made
                                if changes:
                                    with st.expander("View cleaning changes", key=f"expander_file_{section_key}"):
                                        for change in changes:
                                            if change.get("step") == "Fill Missing Values":
                                                st.write(f"✓ Filled {change.get('rows_affected')} missing values in column '{change.get('column')}' with '{change.get('value')}'") 
                                            elif change.get("step") == "Remove Duplicates":
                                                st.write(f"✓ Removed {change.get('rows_affected')} duplicate rows")
                                
                                # Display cleaned data
                                st.subheader(f"Preview of Cleaned Data ('{cleaned_table_name}')")
                                st.dataframe(cleaned_df.head(10), use_container_width=True)
        else:
            st.info("No tables available. Please upload a file.")
        st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.markdown('<div style="padding: 15px; background-color: #f5f5f5; border-radius: 5px;">', unsafe_allow_html=True)
        st.markdown("**Upload a new file**")
        
        uploaded_file = st.file_uploader("Choose a CSV or Excel file", type=["csv", "xlsx", "xls"], key=f"file_uploader_{section_key}")
        
        if uploaded_file is not None:
            # Save the uploaded file to a temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(uploaded_file.name)[1]) as temp:
                temp.write(uploaded_file.getvalue())
                temp_path = temp.name
            
            if st.button("Process File", key=f"process_file_{section_key}"):
                with st.spinner("Processing file..."):
                    # Reset analysis results
                    st.session_state.analysis = None
                    st.session_state.issues = None
                    
                    # Load data from the temporary file
                    st.session_state.table_name, st.session_state.df = load_data_from_file(temp_path)
                    
                    # Display the loaded table
                    if st.session_state.df is not None:
                        st.success(f"File '{uploaded_file.name}' processed successfully!")
                        st.subheader("Preview of loaded data")
                        st.dataframe(st.session_state.df.head(10), use_container_width=True)
                        
                        # Show data cleaning in progress
                        with st.spinner("Cleaning data..."):
                            # Define basic cleaning steps
                            cleaning_steps = [
                                {"type": "fill_missing", "column": "address", "value": "Unknown"},
                                {"type": "remove_duplicates"}
                            ]
                            
                            # Clean the data
                            cleaned_df, changes = clean_data(st.session_state.df, st.session_state.table_name, cleaning_steps)
                            
                            # Save cleaned data to database
                            cleaned_table_name = save_cleaned_data(cleaned_df, st.session_state.table_name)
                            
                            if cleaned_table_name:
                                st.session_state.cleaned_table_name = cleaned_table_name
                                st.session_state.cleaned_df = cleaned_df
                                
                                # Display cleaning summary
                                st.success("Data cleaning completed successfully!")
                                
                                # Show changes made
                                if changes:
                                    with st.expander("View cleaning changes", key=f"expander_file_{section_key}"):
                                        for change in changes:
                                            if change.get("step") == "Fill Missing Values":
                                                st.write(f"✓ Filled {change.get('rows_affected')} missing values in column '{change.get('column')}' with '{change.get('value')}'") 
                                            elif change.get("step") == "Remove Duplicates":
                                                st.write(f"✓ Removed {change.get('rows_affected')} duplicate rows")
                                
                                # Display cleaned data
                                st.subheader(f"Preview of Cleaned Data ('{cleaned_table_name}')")
                                st.dataframe(cleaned_df.head(10), use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)
