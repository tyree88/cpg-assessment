import streamlit as st
import pandas as pd
from util.cleaning import clean_data
from util.analysis import analyze_data
from util.cleaning import identify_data_quality_issues
from util.database import save_cleaned_data

def display_basic_cleaning_options(cleaning_tabs):
    """Display basic cleaning options that were previously in the data loader."""
    with cleaning_tabs[0]:
        st.markdown("**Basic Cleaning Options**")
        st.markdown("Apply common cleaning operations to your data:")
        
        # Option for basic cleaning
        if st.button("Apply Basic Cleaning", key="basic_cleaning"):
            with st.spinner("Cleaning data..."):
                # Define basic cleaning steps
                cleaning_steps = [
                    {"type": "fill_missing", "column": "address", "value": "Unknown"}, 
                    {"type": "remove_duplicates"}
                ]
                
                # Clean the data
                cleaned_df, changes = clean_data(st.session_state.df, st.session_state.table_name, cleaning_steps)
                
                # Save cleaned data to database
                cleaned_table_name = save_cleaned_data(cleaned_df, f"{st.session_state.table_name}_cleaned")
                
                if cleaned_table_name:
                    st.session_state.cleaned_table_name = cleaned_table_name
                    st.session_state.cleaned_df = cleaned_df
                    
                    # Display cleaning summary
                    st.success("Data cleaning completed successfully!")
                    st.success(f"Cleaned data saved to table: {cleaned_table_name}")
                    
                    # Show changes made
                    if changes:
                        with st.expander("View cleaning changes"):
                            for change in changes:
                                if change.get("step") == "Fill Missing Values":
                                    st.write(f"✓ Filled {change.get('rows_affected')} missing values in column '{change.get('column')}' with '{change.get('value')}'") 
                                elif change.get("step") == "Remove Duplicates":
                                    st.write(f"✓ Removed {change.get('rows_affected')} duplicate rows")
                    
                    # Update analysis after cleaning
                    st.session_state.analysis = analyze_data(st.session_state.df, st.session_state.table_name)
                    st.session_state.issues = identify_data_quality_issues(st.session_state.df, st.session_state.analysis, st.session_state.table_name)
        
        st.markdown("---")

def display_data_completeness_options(cleaning_tabs):
    """Display options for handling missing data."""
    with cleaning_tabs[1]:
        st.markdown("**Data Completeness Options**")
        
        # Get columns with missing values
        missing_columns = []
        for col, info in st.session_state.analysis['missing_values'].items():
            if info['count'] > 0:
                missing_columns.append((col, info['count'], info['percent']))
        
        # Sort by percentage of missing values
        missing_columns.sort(key=lambda x: x[2], reverse=True)
        
        if missing_columns:
            st.markdown("The following columns have missing values:")
            
            # Display columns with missing values
            missing_df = pd.DataFrame(missing_columns, columns=["Column", "Missing Count", "Missing %"])
            st.dataframe(missing_df, use_container_width=True)
            
            # Option to fill missing values
            st.markdown("**Fill missing values:**")
            
            # Select column to fill
            cols_with_missing = [col for col, _, _ in missing_columns]
            selected_col = st.selectbox("Select column:", cols_with_missing)
            
            if selected_col:
                st.markdown(f"**Fill missing values in '{selected_col}'**")
                
                # Select fill method
                fill_method = st.radio(
                    "Fill method:",
                    ["Constant value", "Mean", "Median", "Mode", "Forward fill", "Backward fill"],
                    horizontal=True
                )
                
                # Get fill value based on method
                fill_value = None
                if fill_method == "Constant value":
                    fill_value = st.text_input("Enter value:")
                
                # Apply fill
                if st.button("Apply Fill", key="apply_fill"):
                    with st.spinner("Filling missing values..."):
                        # Create cleaning step
                        cleaning_step = {
                            "type": "fill_missing",
                            "column": selected_col,
                            "method": fill_method.lower().replace(" ", "_"),
                            "value": fill_value if fill_method == "Constant value" else None
                        }
                        
                        # Apply cleaning
                        cleaned_df = clean_data(st.session_state.df, st.session_state.table_name, [cleaning_step])
                        if cleaned_df is not None:
                            st.session_state.df = cleaned_df
                            st.success("Missing values filled successfully!")
                            # Update analysis after cleaning
                            st.session_state.analysis = analyze_data(st.session_state.df, st.session_state.table_name)
                            st.session_state.issues = identify_data_quality_issues(st.session_state.df, st.session_state.analysis, st.session_state.table_name)
                            st.experimental_rerun()
        else:
            st.success("No missing values found in the dataset!")


def display_deduplication_options(cleaning_tabs):
    """Display options for deduplicating data and saving cleaned data."""
    with cleaning_tabs[2]:
        # Option to remove duplicates
        st.markdown("**Deduplication options:**")
        
        if st.checkbox("Remove duplicate rows"):
            # Select columns to consider for duplicates
            st.markdown("Select columns to consider for identifying duplicates:")
            all_columns = st.session_state.df.columns.tolist()
            selected_columns = st.multiselect("Columns:", all_columns, default=all_columns)
            
            # Keep first or last occurrence
            keep_option = st.radio("Keep:", ["First occurrence", "Last occurrence"], horizontal=True)
            keep = "first" if keep_option == "First occurrence" else "last"
            
            # Apply deduplication
            if st.button("Remove Duplicates", key="remove_duplicates"):
                with st.spinner("Removing duplicates..."):
                    # Create cleaning step
                    cleaning_step = {
                        "type": "remove_duplicates",
                        "columns": selected_columns,
                        "keep": keep
                    }
                    
                    # Apply cleaning
                    cleaned_df = clean_data(st.session_state.df, st.session_state.table_name, [cleaning_step])
                    if cleaned_df is not None:
                        # Count removed duplicates
                        removed_count = len(st.session_state.df) - len(cleaned_df)
                        
                        st.session_state.df = cleaned_df
                        st.success(f"Successfully removed {removed_count} duplicate rows!")
                        # Update analysis after cleaning
                        st.session_state.analysis = analyze_data(st.session_state.df, st.session_state.table_name)
                        st.session_state.issues = identify_data_quality_issues(st.session_state.df, st.session_state.analysis, st.session_state.table_name)
                        st.experimental_rerun()
        
        # Option to save cleaned data
        st.markdown("---")
        st.markdown("**Save cleaned data:**")
        
        # Enter new table name
        new_table_name = st.text_input("New table name:", value=f"{st.session_state.table_name}_cleaned")
        
        # Save button
        if st.button("Save Cleaned Data", key="save_cleaned_data"):
            with st.spinner("Saving data..."):
                # Save the cleaned data
                success = save_cleaned_data(st.session_state.df, new_table_name)
                if success:
                    st.success(f"Data saved successfully as '{new_table_name}'!")
                else:
                    st.error("Failed to save data. Please check the table name and try again.")
