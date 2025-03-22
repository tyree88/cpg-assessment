#!/usr/bin/env python3
"""
Visualization Utilities

Refactored visualization functions for the DataPlor application with improved
organization, caching, and performance optimizations.
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, Any, Optional, List, Union, Tuple
from enum import Enum, auto
import io
import base64


class PlotType(Enum):
    """Enum for different types of plots supported by the visualization module."""
    BAR = auto()
    LINE = auto()
    SCATTER = auto()
    PIE = auto()
    HEATMAP = auto()
    MISSING_VALUES = auto()
    CORRELATION = auto()
    DISTRIBUTION = auto()
    BOX = auto()
    VIOLIN = auto()


# Global configuration
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'output', 'plots')
os.makedirs(OUTPUT_DIR, exist_ok=True)


def setup_plot_style(style: str = "whitegrid", 
                     figsize: Tuple[int, int] = (10, 6), 
                     font_size: int = 12) -> None:
    """
    Set up consistent plot styling.
    
    Args:
        style: Seaborn style theme
        figsize: Default figure size
        font_size: Default font size
    """
    sns.set_theme(style=style)
    plt.rcParams["figure.figsize"] = figsize
    plt.rcParams["font.size"] = font_size


def save_figure(filename: str, title: Optional[str] = None) -> str:
    """
    Save a figure to the output directory.
    
    Args:
        filename: Base filename for the saved figure
        title: Optional title to add to the plot
        
    Returns:
        Path to the saved figure
    """
    if title:
        plt.title(title)
    
    plt.tight_layout()
    
    # Ensure filename has proper extension
    if not filename.endswith('.png'):
        filename += '.png'
    
    # Create full path
    filepath = os.path.join(OUTPUT_DIR, filename)
    
    # Save the figure
    plt.savefig(filepath, dpi=300, bbox_inches='tight')
    plt.close()
    
    return filepath


def get_figure_as_base64() -> str:
    """
    Get the current figure as a base64 encoded string for embedding in HTML.
    
    Returns:
        Base64 encoded string of the figure
    """
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # Encode the image
    buf.seek(0)
    img_str = base64.b64encode(buf.read()).decode('utf-8')
    
    return f"data:image/png;base64,{img_str}"


# Bar Plot Functions
def create_bar_plot(data: pd.DataFrame, 
                   x_col: str, 
                   y_col: str, 
                   title: str = "Bar Plot",
                   filename: str = "bar_plot",
                   max_items: int = 10,
                   sort_by: Optional[str] = None,
                   ascending: bool = False,
                   color: str = 'steelblue',
                   horizontal: bool = False,
                   as_base64: bool = False) -> str:
    """
    Create a bar plot from DataFrame.
    
    Args:
        data: DataFrame containing data to plot
        x_col: Column name for x-axis values
        y_col: Column name for y-axis values
        title: Title for the plot
        filename: Name of the file to save
        max_items: Maximum number of items to display
        sort_by: Column to sort by (defaults to x_col if None)
        ascending: Sort order
        color: Bar color
        horizontal: If True, creates a horizontal bar plot
        as_base64: If True, returns base64 encoded image instead of file path
        
    Returns:
        Path to the saved figure or base64 encoded image
    """
    if data is None or data.empty:
        return ""
    
    # Sort and limit data
    sort_column = sort_by if sort_by else y_col
    plot_data = data.sort_values(sort_column, ascending=ascending).head(max_items)
    
    # Create plot
    plt.figure(figsize=(10, 6))
    
    if horizontal:
        # For horizontal bar plot, swap x and y
        sns.barplot(y=plot_data[x_col], x=plot_data[y_col], color=color)
    else:
        sns.barplot(x=plot_data[x_col], y=plot_data[y_col], color=color)
    
    # Rotate x-axis labels if needed
    if not horizontal and len(plot_data) > 5:
        plt.xticks(rotation=45, ha='right')
    
    plt.title(title)
    plt.tight_layout()
    
    if as_base64:
        return get_figure_as_base64()
    else:
        return save_figure(filename, None)  # Title already added


# Line Plot Functions
def create_line_plot(data: pd.DataFrame, 
                    x_col: str, 
                    y_cols: List[str], 
                    title: str = "Line Plot",
                    filename: str = "line_plot",
                    as_base64: bool = False) -> str:
    """
    Create a line plot from DataFrame.
    
    Args:
        data: DataFrame containing data to plot
        x_col: Column name for x-axis values
        y_cols: List of column names for y-axis values
        title: Title for the plot
        filename: Name of the file to save
        as_base64: If True, returns base64 encoded image instead of file path
        
    Returns:
        Path to the saved figure or base64 encoded image
    """
    if data is None or data.empty:
        return ""
    
    # Create plot
    plt.figure(figsize=(10, 6))
    for col in y_cols:
        plt.plot(data[x_col], data[col], marker='o', label=col)
    
    plt.legend()
    plt.title(title)
    plt.tight_layout()
    
    if as_base64:
        return get_figure_as_base64()
    else:
        return save_figure(filename, None)  # Title already added


# Missing Values Plot
def plot_missing_values(missing_values: Dict[str, Dict[str, Any]], 
                       max_cols: int = 10, 
                       title: str = "Missing Values by Column (%)",
                       filename: str = "missing_values",
                       as_base64: bool = False) -> str:
    """
    Create a bar chart of missing values percentages.
    
    Args:
        missing_values: Dictionary of column names to missing value stats
        max_cols: Maximum number of columns to display
        title: Title for the plot
        filename: Name of the file to save
        as_base64: If True, returns base64 encoded image instead of file path
        
    Returns:
        Path to the saved figure or base64 encoded image
    """
    # Convert dictionary to DataFrame
    data = []
    for col, stats in missing_values.items():
        if 'missing_percent' in stats:
            data.append({
                'column': col,
                'missing_percent': stats['missing_percent']
            })
    
    if not data:
        return ""
    
    df = pd.DataFrame(data)
    
    # Sort by missing percentage and take top columns
    df = df.sort_values('missing_percent', ascending=False).head(max_cols)
    
    # Create horizontal bar chart
    return create_bar_plot(
        data=df,
        x_col='column',
        y_col='missing_percent',
        title=title,
        filename=filename,
        horizontal=True,
        as_base64=as_base64
    )


# Retail Segments Plot
def plot_retail_segments(segments_df: pd.DataFrame, 
                        count_col: str = "location_count", 
                        category_col: str = "category",
                        max_segments: int = 10,
                        title: str = "Top Retail Segments",
                        filename: str = "retail_segments",
                        as_base64: bool = False) -> str:
    """
    Create a bar chart of retail segments.
    
    Args:
        segments_df: DataFrame containing segment data
        count_col: Column name for count values
        category_col: Column name for category labels
        max_segments: Maximum number of segments to display
        title: Title for the plot
        filename: Name of the file to save
        as_base64: If True, returns base64 encoded image instead of file path
        
    Returns:
        Path to the saved figure or base64 encoded image
    """
    return create_bar_plot(
        data=segments_df,
        x_col=category_col,
        y_col=count_col,
        title=title,
        filename=filename,
        max_items=max_segments,
        sort_by=count_col,
        ascending=False,
        as_base64=as_base64
    )


# Territory Coverage Plot
def plot_territory_coverage(territory_df: pd.DataFrame,
                           location_col: str = "location_count",
                           city_col: str = "city",
                           max_cities: int = 10,
                           title: str = "Territory Coverage",
                           filename: str = "territory_coverage",
                           as_base64: bool = False) -> str:
    """
    Create a bar chart of territory coverage.
    
    Args:
        territory_df: DataFrame containing territory data
        location_col: Column name for location count
        city_col: Column name for city names
        max_cities: Maximum number of cities to display
        title: Title for the plot
        filename: Name of the file to save
        as_base64: If True, returns base64 encoded image instead of file path
        
    Returns:
        Path to the saved figure or base64 encoded image
    """
    return create_bar_plot(
        data=territory_df,
        x_col=city_col,
        y_col=location_col,
        title=title,
        filename=filename,
        max_items=max_cities,
        sort_by=location_col,
        ascending=False,
        as_base64=as_base64
    )


# Correlation Heatmap
def plot_correlation_matrix(df: pd.DataFrame,
                           numeric_only: bool = True,
                           title: str = "Correlation Matrix",
                           filename: str = "correlation_matrix",
                           as_base64: bool = False) -> str:
    """
    Create a heatmap of correlation matrix.
    
    Args:
        df: DataFrame to analyze
        numeric_only: If True, only include numeric columns
        title: Title for the plot
        filename: Name of the file to save
        as_base64: If True, returns base64 encoded image instead of file path
        
    Returns:
        Path to the saved figure or base64 encoded image
    """
    if df is None or df.empty:
        return ""
    
    # Get numeric columns if requested
    if numeric_only:
        df = df.select_dtypes(include=['number'])
    
    if df.empty:
        return ""
    
    # Calculate correlation matrix
    corr = df.corr()
    
    # Create heatmap
    plt.figure(figsize=(12, 10))
    mask = np.triu(np.ones_like(corr, dtype=bool))
    sns.heatmap(corr, mask=mask, annot=True, fmt=".2f", cmap="coolwarm", 
                square=True, linewidths=.5, cbar_kws={"shrink": .5})
    
    plt.title(title)
    plt.tight_layout()
    
    if as_base64:
        return get_figure_as_base64()
    else:
        return save_figure(filename, None)  # Title already added


# Distribution Plot
def plot_distribution(df: pd.DataFrame,
                     column: str,
                     bins: int = 30,
                     kde: bool = True,
                     title: Optional[str] = None,
                     filename: Optional[str] = None,
                     as_base64: bool = False) -> str:
    """
    Create a distribution plot for a numeric column.
    
    Args:
        df: DataFrame containing the data
        column: Column name to plot
        bins: Number of histogram bins
        kde: Whether to include KDE curve
        title: Title for the plot (defaults to column name if None)
        filename: Name of the file to save (defaults to column name if None)
        as_base64: If True, returns base64 encoded image instead of file path
        
    Returns:
        Path to the saved figure or base64 encoded image
    """
    if df is None or df.empty or column not in df.columns:
        return ""
    
    # Set defaults
    if title is None:
        title = f"Distribution of {column}"
    if filename is None:
        filename = f"distribution_{column.lower().replace(' ', '_')}"
    
    # Create plot
    plt.figure(figsize=(10, 6))
    sns.histplot(df[column].dropna(), bins=bins, kde=kde)
    
    plt.title(title)
    plt.tight_layout()
    
    if as_base64:
        return get_figure_as_base64()
    else:
        return save_figure(filename, None)  # Title already added


# Initialize default plot style
setup_plot_style()
