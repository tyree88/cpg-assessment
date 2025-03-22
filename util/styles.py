#!/usr/bin/env python3
"""
Styles Module

Contains CSS styles for the DataPlor application UI.
"""

import streamlit as st

def apply_base_styles():
    """Apply base styles to the Streamlit application."""
    st.markdown("""
    <style>
        /* Base Styles */
        .main-header {font-size: 2.2rem !important; color: #1E88E5; font-weight: 600; margin-bottom: 0.5rem;}
        .sub-header {font-size: 1.4rem !important; color: #424242; margin-bottom: 0.8rem;}
        .section-header {font-size: 1.2rem !important; color: #424242; margin: 1rem 0 0.5rem 0; font-weight: 600;}
        
        /* Card Styles */
        .metric-card {
            background-color: #f8f9fa; 
            border-radius: 8px; 
            padding: 18px; 
            margin: 12px 0; 
            box-shadow: 0 2px 5px rgba(0,0,0,0.05);
            transition: transform 0.2s, box-shadow 0.2s;
        }
        .metric-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 8px rgba(0,0,0,0.1);
        }
        
        /* Issue Status Colors */
        .critical-issue {color: #D32F2F; font-weight: 500;}
        .warning-issue {color: #FF9800; font-weight: 500;}
        .good-quality {color: #4CAF50; font-weight: 500;}
        .info-text {color: #1976D2; font-weight: 500;}
        
        /* Tab Styling */
        .stTabs [data-baseweb="tab-list"] {gap: 24px;}
        .stTabs [data-baseweb="tab"] {height: 50px; white-space: pre-wrap;}
        .stTabs [aria-selected="true"] {background-color: #f0f2f6; border-radius: 4px 4px 0 0;}
        
        /* Expander Styling */
        .stExpander {border: 1px solid #f0f2f6; border-radius: 8px; margin-bottom: 1rem;}
    </style>
    """, unsafe_allow_html=True)

def apply_component_styles():
    """Apply component-specific styles."""
    st.markdown("""
    <style>
        /* Button Styling */
        div.stButton > button {
            background-color: #1E88E5;
            color: white;
            border-radius: 4px;
            border: none;
            padding: 0.5rem 1rem;
            font-weight: 500;
        }
        div.stButton > button:hover {
            background-color: #1976D2;
            border: none;
        }
        
        /* Progress Bar Styling */
        div.stProgress > div > div > div > div {
            background-color: #4CAF50;
        }
        
        /* Data Table Styling */
        .dataframe {
            border-collapse: collapse;
            margin: 25px 0;
            font-size: 0.9em;
            font-family: sans-serif;
            min-width: 400px;
            box-shadow: 0 0 20px rgba(0, 0, 0, 0.15);
            border-radius: 8px;
            overflow: hidden;
        }
        .dataframe thead tr {
            background-color: #1E88E5;
            color: #ffffff;
            text-align: left;
        }
        .dataframe th, .dataframe td {
            padding: 12px 15px;
        }
        .dataframe tbody tr {
            border-bottom: 1px solid #dddddd;
        }
        .dataframe tbody tr:nth-of-type(even) {
            background-color: #f3f3f3;
        }
        .dataframe tbody tr:last-of-type {
            border-bottom: 2px solid #1E88E5;
        }
    </style>
    """, unsafe_allow_html=True)

def apply_all_styles():
    """Apply all styles to the application."""
    apply_base_styles()
    apply_component_styles()
