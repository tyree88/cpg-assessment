import streamlit as st

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

def display_app_header():
    """Display the application header and title."""
    st.markdown('<h1 class="main-header">DataPlor - CPG Data Quality Assessment</h1>', unsafe_allow_html=True)

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
