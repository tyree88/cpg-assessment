#!/usr/bin/env python3
"""
Quality Visualizations Module

Provides enhanced visualizations specifically for data quality assessment.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import streamlit as st
from typing import Dict, Any, List, Optional, Union
import numpy as np
import altair as alt

# Simple logging with print statements

# Set default plot styles
plt.style.use('ggplot')


def plot_missing_values_heatmap(df: pd.DataFrame, max_cols: int = 20) -> alt.Chart:
    """
    Create an interactive heatmap of missing values.
    
    Args:
        df: DataFrame to analyze
        max_cols: Maximum number of columns to display
        
    Returns:
        Altair chart object
    """
    # Calculate missing values
    missing_data = pd.DataFrame(df.isnull().sum() / len(df) * 100).reset_index()
    missing_data.columns = ['column', 'percent_missing']
    
    # Sort and limit to max_cols
    missing_data = missing_data.sort_values('percent_missing', ascending=False).head(max_cols)
    
    # Create color scale
    color_scale = alt.Scale(
        domain=[0, 25, 50, 75, 100],
        range=['#4CAF50', '#8BC34A', '#FFEB3B', '#FF9800', '#F44336']
    )
    
    # Create the heatmap
    chart = alt.Chart(missing_data).mark_rect().encode(
        y=alt.Y('column:N', title='Column', sort='-x'),
        x=alt.X('percent_missing:Q', title='Missing Values (%)'),
        color=alt.Color('percent_missing:Q', scale=color_scale, legend=alt.Legend(title="Missing %")),
        tooltip=['column', alt.Tooltip('percent_missing:Q', format='.1f')]
    ).properties(
        width=600,
        height=max(300, missing_data.shape[0] * 20),
        title='Missing Values by Column'
    ).interactive()
    
    return chart


def plot_quality_score_gauge(score: float, title: str = "Overall Quality Score") -> None:
    """
    Create a gauge chart for quality score visualization.
    
    Args:
        score: Quality score (0-100)
        title: Title for the chart
    """
    # Ensure score is within bounds
    score = max(0, min(100, score))
    
    # Create figure and axis
    fig, ax = plt.subplots(figsize=(8, 4), subplot_kw={'projection': 'polar'})
    
    # Define gauge properties
    gauge_min, gauge_max = 0, 100
    theta = np.linspace(np.pi, 0, 100)
    
    # Color ranges for different quality levels
    colors = [(0.9, 0.1, 0.1), (0.9, 0.6, 0.1), (0.1, 0.7, 0.1)]  # Red, Orange, Green
    
    # Background arcs
    ax.bar(
        x=np.pi/2, 
        width=np.pi,
        bottom=0.7,
        height=0.1, 
        color='lightgrey',
        edgecolor='white',
        alpha=0.8
    )
    
    # Colored arc based on score
    if score <= 50:
        color_idx = 0  # Red
    elif score <= 75:
        color_idx = 1  # Orange
    else:
        color_idx = 2  # Green
        
    ax.bar(
        x=np.pi/2,
        width=np.pi * score/100,
        bottom=0.7,
        height=0.1,
        color=colors[color_idx],
        edgecolor='white'
    )
    
    # Add score text
    ax.text(0, 0, f"{score:.1f}%", ha='center', va='center', fontsize=24, fontweight='bold')
    ax.text(0, -0.2, title, ha='center', va='center', fontsize=12)
    
    # Clean up the chart
    ax.set_axis_off()
    
    # Display in Streamlit
    st.pyplot(fig)


def plot_issue_breakdown(issues: Dict[str, List[Any]]) -> alt.Chart:
    """
    Create an interactive bar chart showing breakdown of issues by severity.
    
    Args:
        issues: Dictionary with issues categorized by severity
        
    Returns:
        Altair chart object
    """
    # Prepare data
    issue_counts = {
        'critical': len(issues.get('critical', [])),
        'warning': len(issues.get('warning', [])),
        'info': len(issues.get('info', []))
    }
    
    issue_df = pd.DataFrame({
        'severity': list(issue_counts.keys()),
        'count': list(issue_counts.values())
    })
    
    # Define colors
    colors = ['#F44336', '#FF9800', '#2196F3']
    
    # Create chart
    chart = alt.Chart(issue_df).mark_bar().encode(
        x=alt.X('severity:N', title='Severity Level', sort=['critical', 'warning', 'info']),
        y=alt.Y('count:Q', title='Number of Issues'),
        color=alt.Color('severity:N', scale=alt.Scale(domain=['critical', 'warning', 'info'], range=colors)),
        tooltip=['severity', 'count']
    ).properties(
        width=500,
        height=300,
        title='Data Quality Issues by Severity'
    ).interactive()
    
    return chart


def plot_column_quality_scores(column_scores: Dict[str, float], max_cols: int = 10) -> alt.Chart:
    """
    Create an interactive bar chart of column quality scores.
    
    Args:
        column_scores: Dictionary mapping column names to quality scores
        max_cols: Maximum number of columns to display
        
    Returns:
        Altair chart object
    """
    # Prepare data
    score_df = pd.DataFrame({
        'column': list(column_scores.keys()),
        'score': list(column_scores.values())
    })
    
    # Sort and limit to max_cols
    score_df = score_df.sort_values('score').tail(max_cols)
    
    # Create color scale
    color_scale = alt.Scale(
        domain=[0, 50, 75, 100],
        range=['#F44336', '#FF9800', '#8BC34A', '#4CAF50']
    )
    
    # Create chart
    chart = alt.Chart(score_df).mark_bar().encode(
        y=alt.Y('column:N', title='Column', sort='-x'),
        x=alt.X('score:Q', title='Quality Score', scale=alt.Scale(domain=[0, 100])),
        color=alt.Color('score:Q', scale=color_scale, legend=alt.Legend(title="Score")),
        tooltip=['column', alt.Tooltip('score:Q', format='.1f')]
    ).properties(
        width=600,
        height=max(300, score_df.shape[0] * 25),
        title='Column Quality Scores'
    ).interactive()
    
    return chart
