#!/usr/bin/env python3
"""
UI Components Module

Provides reusable UI components for the DataPlor application.
"""

import streamlit as st
import pandas as pd
from typing import Dict, Any, List, Callable

def create_metric_card(title: str, value: Any, description: str = "", icon: str = "", is_good: bool = True) -> None:
    """
    Create a styled metric card with title, value, and optional description.
    
    Args:
        title: Title of the metric
        value: Value to display
        description: Optional description text
        icon: Optional emoji icon
        is_good: Whether the metric represents a good value (affects styling)
    """
    # Determine status class based on is_good
    status_class = "good-quality" if is_good else "critical-issue"
    
    # Create the card
    st.markdown(f"""
    <div class="metric-card">
        <div style="display: flex; justify-content: space-between; align-items: center;">
            <h3 style="margin: 0;">{title}</h3>
            <span style="font-size: 1.5rem;">{icon}</span>
        </div>
        <p class="{status_class}" style="font-size: 2rem; margin: 0.5rem 0;">{value}</p>
        <p style="margin: 0; color: #666;">{description}</p>
    </div>
    """, unsafe_allow_html=True)


def create_progress_steps(steps: List[str], active_step: int, completed_steps: set) -> None:
    """
    Create a progress indicator for multi-step workflows.
    
    Args:
        steps: List of step names
        active_step: Index of the currently active step (1-based)
        completed_steps: Set of completed step indices (1-based)
    """
    # Create columns for each step
    cols = st.columns(len(steps))
    
    # Display each step
    for i, (col, step) in enumerate(zip(cols, steps), 1):
        with col:
            if i < active_step:
                # Completed step
                st.markdown(f"""
                <div style="text-align: center;">
                    <div class="wizard-step" style="background-color: #4CAF50; color: white; margin: 0 auto;">✓</div>
                    <p style="margin-top: 0.5rem; font-size: 0.8rem; color: #4CAF50;">{step}</p>
                </div>
                """, unsafe_allow_html=True)
            elif i == active_step:
                # Active step
                st.markdown(f"""
                <div style="text-align: center;">
                    <div class="wizard-step active" style="margin: 0 auto;">{i}</div>
                    <p style="margin-top: 0.5rem; font-size: 0.8rem; font-weight: bold;">{step}</p>
                </div>
                """, unsafe_allow_html=True)
            else:
                # Future step
                st.markdown(f"""
                <div style="text-align: center;">
                    <div class="wizard-step" style="margin: 0 auto;">{i}</div>
                    <p style="margin-top: 0.5rem; font-size: 0.8rem; color: #999;">{step}</p>
                </div>
                """, unsafe_allow_html=True)


def create_info_box(title: str, content: str, box_type: str = "info") -> None:
    """
    Create a styled info box with title and content.
    
    Args:
        title: Title of the info box
        content: Content text
        box_type: Type of box (info, warning, success, error)
    """
    # Define styles based on box_type
    styles = {
        "info": {"color": "#1976D2", "bg": "#E3F2FD", "icon": "ℹ️"},
        "warning": {"color": "#FF9800", "bg": "#FFF3E0", "icon": "⚠️"},
        "success": {"color": "#4CAF50", "bg": "#E8F5E9", "icon": "✅"},
        "error": {"color": "#D32F2F", "bg": "#FFEBEE", "icon": "❌"}
    }
    
    style = styles.get(box_type, styles["info"])
    
    # Create the info box
    st.markdown(f"""
    <div style="background-color: {style['bg']}; padding: 1rem; border-radius: 0.5rem; margin: 1rem 0;">
        <div style="display: flex; align-items: center; margin-bottom: 0.5rem;">
            <span style="font-size: 1.2rem; margin-right: 0.5rem;">{style['icon']}</span>
            <h4 style="margin: 0; color: {style['color']};">{title}</h4>
        </div>
        <p style="margin: 0; color: #333;">{content}</p>
    </div>
    """, unsafe_allow_html=True)


def create_data_card(title: str, data: pd.DataFrame, max_rows: int = 5, show_index: bool = False) -> None:
    """
    Create a styled card to display a DataFrame.
    
    Args:
        title: Title of the card
        data: DataFrame to display
        max_rows: Maximum number of rows to display
        show_index: Whether to show the index column
    """
    # Create the card
    st.markdown(f"""
    <div class="metric-card">
        <h3 style="margin-top: 0;">{title}</h3>
    """, unsafe_allow_html=True)
    
    # Display the DataFrame
    st.dataframe(data.head(max_rows), use_container_width=True, hide_index=not show_index)
    
    # Add a footer with row count info
    total_rows = len(data)
    st.markdown(f"""
        <p style="margin: 0.5rem 0 0 0; color: #666; text-align: right;">
            Showing {min(max_rows, total_rows)} of {total_rows} rows
        </p>
    </div>
    """, unsafe_allow_html=True)


def create_action_card(title: str, description: str, action_label: str, action_callback: Callable, icon: str = "") -> None:
    """
    Create a card with an action button.
    
    Args:
        title: Title of the card
        description: Description text
        action_label: Label for the action button
        action_callback: Callback function for the button
        icon: Optional emoji icon
    """
    # Create the card
    st.markdown(f"""
    <div class="metric-card">
        <div style="display: flex; justify-content: space-between; align-items: center;">
            <h3 style="margin: 0;">{title}</h3>
            <span style="font-size: 1.5rem;">{icon}</span>
        </div>
        <p style="margin: 0.5rem 0; color: #666;">{description}</p>
    """, unsafe_allow_html=True)
    
    # Add the action button
    st.button(action_label, on_click=action_callback, key=f"action_{title.lower().replace(' ', '_')}")
    
    # Close the card
    st.markdown("</div>", unsafe_allow_html=True)


def create_expandable_section(title: str, content_func: Callable, expanded: bool = False, icon: str = "") -> None:
    """
    Create an expandable section with custom content.
    
    Args:
        title: Title of the section
        content_func: Function that generates the content
        expanded: Whether the section is expanded by default
        icon: Optional emoji icon
    """
    # Create the title with icon if provided
    display_title = f"{icon} {title}" if icon else title
    
    # Create the expander
    with st.expander(display_title, expanded=expanded):
        # Call the content function to generate content
        content_func()


def create_tab_navigation(tabs: List[Dict[str, Any]]) -> None:
    """
    Create a custom tab navigation.
    
    Args:
        tabs: List of tab dictionaries with 'title', 'icon', and 'content_func' keys
    """
    # Create tab buttons
    col_tabs = st.columns(len(tabs))
    
    # Get current tab from session state
    if 'current_tab' not in st.session_state:
        st.session_state.current_tab = 0
    
    # Display tab buttons
    for i, (col, tab) in enumerate(zip(col_tabs, tabs)):
        with col:
            is_active = st.session_state.current_tab == i
            button_style = "primary" if is_active else "secondary"
            if st.button(f"{tab['icon']} {tab['title']}", type=button_style, key=f"tab_{i}"):
                st.session_state.current_tab = i
    
    # Display content for the active tab
    st.markdown("---")
    tabs[st.session_state.current_tab]['content_func']()
