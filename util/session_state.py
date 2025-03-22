"""
Session State Management

This module provides utility functions for managing Streamlit session state
in a more consistent and centralized way.
"""

import streamlit as st
from typing import Any, Dict, List, Optional, Set, Union
import time


def initialize_session_state():
    """Initialize all required session state variables with default values."""
    # Data state variables
    if 'table_name' not in st.session_state:
        st.session_state.table_name = None
    if 'df' not in st.session_state:
        st.session_state.df = None
    if 'analysis' not in st.session_state:
        st.session_state.analysis = None
    if 'issues' not in st.session_state:
        st.session_state.issues = None
    
    # UI state variables for progressive disclosure
    if 'show_advanced_options' not in st.session_state:
        st.session_state.show_advanced_options = False
    if 'active_step' not in st.session_state:
        st.session_state.active_step = 1
    if 'completed_steps' not in st.session_state:
        st.session_state.completed_steps = set()
    if 'last_interaction' not in st.session_state:
        st.session_state.last_interaction = time.time()


def get_session_state(key: str, default: Any = None) -> Any:
    """
    Get a value from session state with a default fallback.
    
    Args:
        key: The session state key to retrieve
        default: Default value to return if key doesn't exist
        
    Returns:
        The value from session state or the default
    """
    if key in st.session_state:
        return st.session_state[key]
    return default


def set_session_state(key: str, value: Any) -> None:
    """
    Set a value in session state.
    
    Args:
        key: The session state key to set
        value: The value to store
    """
    st.session_state[key] = value


def update_session_state(data: Dict[str, Any]) -> None:
    """
    Update multiple session state values at once.
    
    Args:
        data: Dictionary of key-value pairs to update in session state
    """
    for key, value in data.items():
        st.session_state[key] = value


def reset_analysis_state() -> None:
    """Reset analysis-related session state variables."""
    st.session_state.analysis = None
    st.session_state.issues = None
    
    # Reset progress tracking
    st.session_state.active_step = 1
    st.session_state.completed_steps = set()


def mark_step_complete(step_name: str, advance_to: Optional[int] = None) -> None:
    """
    Mark a workflow step as complete and optionally advance to next step.
    
    Args:
        step_name: Name of the step to mark as complete
        advance_to: Optional step number to advance to
    """
    if 'completed_steps' not in st.session_state:
        st.session_state.completed_steps = set()
    
    st.session_state.completed_steps.add(step_name)
    
    # Map old step numbers to new step numbers (after removing 'Explore Issues')
    # Old: 1=Load, 2=Analyze, 3=Explore, 4=Clean, 5=Report
    # New: 1=Load, 2=Analyze, 3=Clean, 4=Report
    if advance_to is not None:
        if advance_to == 4:  # If trying to go to old step 4 (Clean)
            st.session_state.active_step = 3  # Go to new step 3 (Clean)
        elif advance_to == 5:  # If trying to go to old step 5 (Report)
            st.session_state.active_step = 4  # Go to new step 4 (Report)
        else:
            st.session_state.active_step = advance_to


def is_step_complete(step_name: str) -> bool:
    """
    Check if a workflow step is marked as complete.
    
    Args:
        step_name: Name of the step to check
        
    Returns:
        True if the step is complete, False otherwise
    """
    if 'completed_steps' not in st.session_state:
        return False
    
    return step_name in st.session_state.completed_steps


def get_completed_steps() -> Set[str]:
    """
    Get the set of completed workflow steps.
    
    Returns:
        Set of completed step names
    """
    if 'completed_steps' not in st.session_state:
        st.session_state.completed_steps = set()
    
    return st.session_state.completed_steps


def get_active_step() -> int:
    """
    Get the current active workflow step.
    
    Returns:
        Current active step number
    """
    if 'active_step' not in st.session_state:
        st.session_state.active_step = 1
    
    return st.session_state.active_step
