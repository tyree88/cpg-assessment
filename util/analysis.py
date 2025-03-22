#!/usr/bin/env python3
"""
Data Analysis Module

Provides functions for analyzing data with a focus on CPG and point-of-interest data.
Refactored to use smaller, focused tasks for better maintainability and performance.
"""

import pandas as pd
import re
from typing import Dict, Any, List, Optional, Tuple
from prefect import task
from util.visualization import plot_missing_values, plot_retail_segments


@task(name="Calculate Basic Statistics", tags=["data-analysis"])
def calculate_basic_statistics(df: pd.DataFrame) -> Dict[str, Any]:
    """Calculate basic dataframe statistics."""
    return {
        'row_count': len(df),
        'column_count': len(df.columns),
        'column_types': {col: str(df[col].dtype) for col in df.columns}
    }


@task(name="Analyze Missing Values", tags=["data-analysis"])
def analyze_missing_values(df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    """Analyze missing values in the dataframe."""
    missing_values = {}
    for col in df.columns:
        missing_count = df[col].isna().sum()
        missing_percent = (missing_count / len(df)) * 100
        missing_values[col] = {
            'count': int(missing_count),
            'percent': round(missing_percent, 2)
        }
    
    # Overall missing data percentage
    total_cells = len(df) * len(df.columns)
    total_missing = sum(info['count'] for info in missing_values.values())
    overall_missing_percent = round((total_missing / total_cells) * 100, 2)
    
    return {
        'column_missing': missing_values,
        'overall_missing_percent': overall_missing_percent
    }


@task(name="Analyze Duplicate Rows", tags=["data-analysis"])
def analyze_duplicates(df: pd.DataFrame) -> Dict[str, Any]:
    """Analyze duplicate rows in the dataframe."""
    duplicate_count = df.duplicated().sum()
    return {
        'count': int(duplicate_count),
        'percent': round((duplicate_count / len(df)) * 100, 2)
    }


@task(name="Analyze Location Data", tags=["data-analysis", "cpg"])
def analyze_location_data(df: pd.DataFrame) -> Dict[str, Any]:
    """Analyze location data (latitude/longitude) if present."""
    location_data = {}
    
    # Find latitude/longitude columns
    lat_cols = [col for col in df.columns if col.lower() in ['latitude', 'lat']]
    lng_cols = [col for col in df.columns if col.lower() in ['longitude', 'long', 'lng']]
    
    if lat_cols and lng_cols:
        lat_col = lat_cols[0]
        lng_col = lng_cols[0]
        
        # Check for valid coordinates
        valid_lat = df[lat_col].between(-90, 90, inclusive='both')
        valid_lng = df[lng_col].between(-180, 180, inclusive='both')
        
        location_data['valid_coordinates'] = {
            'count': int(valid_lat.sum() & valid_lng.sum()),
            'percent': round(((valid_lat.sum() & valid_lng.sum()) / len(df)) * 100, 2)
        }
        
        location_data['invalid_coordinates'] = {
            'count': int(len(df) - (valid_lat.sum() & valid_lng.sum())),
            'percent': round(((len(df) - (valid_lat.sum() & valid_lng.sum())) / len(df)) * 100, 2)
        }
        
        # Check for null coordinates
        null_lat = df[lat_col].isna()
        null_lng = df[lng_col].isna()
        
        location_data['null_coordinates'] = {
            'count': int(null_lat.sum() | null_lng.sum()),
            'percent': round(((null_lat.sum() | null_lng.sum()) / len(df)) * 100, 2)
        }
    
    return location_data


@task(name="Analyze Business Names", tags=["data-analysis", "cpg"])
def analyze_business_names(df: pd.DataFrame) -> Dict[str, Any]:
    """Analyze business name data if present."""
    business_names = {}
    
    # Find name columns
    name_cols = [col for col in df.columns if col.lower() in ['name', 'business_name', 'store_name', 'poi_name']]
    if name_cols:
        name_col = name_cols[0]
        
        # Check for missing business names
        missing_names = df[name_col].isna().sum()
        business_names['missing'] = {
            'count': int(missing_names),
            'percent': round((missing_names / len(df)) * 100, 2)
        }
        
        # Check for duplicate business names
        duplicate_names = df[name_col].duplicated().sum()
        business_names['duplicates'] = {
            'count': int(duplicate_names),
            'percent': round((duplicate_names / len(df)) * 100, 2)
        }
    
    return business_names


@task(name="Analyze Categories", tags=["data-analysis", "cpg"])
def analyze_categories(df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
    """Analyze category/classification data if present."""
    categories = {}
    visualizations = {}
    
    # Find category columns
    category_cols = [col for col in df.columns if col.lower() in ['category', 'categories', 'classification', 'type', 'business_type']]
    if category_cols:
        category_col = category_cols[0]
        
        # Check for missing categories
        missing_categories = df[category_col].isna().sum()
        categories['missing'] = {
            'count': int(missing_categories),
            'percent': round((missing_categories / len(df)) * 100, 2)
        }
        
        # Get category distribution
        category_counts = df[category_col].value_counts().to_dict()
        categories['distribution'] = {
            str(k): int(v) for k, v in category_counts.items() if pd.notna(k)
        }
        
        # Generate category visualization
        try:
            # Create a DataFrame for visualization
            segments_df = pd.DataFrame({
                'category': list(categories['distribution'].keys()),
                'location_count': list(categories['distribution'].values())
            }).sort_values('location_count', ascending=False)
            
            plot_path = plot_retail_segments(segments_df, 
                                           title=f"Category Distribution in {table_name}")
            visualizations['categories'] = plot_path
        except Exception as e:
            print(f"Error generating category visualization: {str(e)}")
    
    return {
        'categories': categories,
        'visualizations': visualizations
    }


@task(name="Analyze Address Data", tags=["data-analysis", "cpg"])
def analyze_addresses(df: pd.DataFrame) -> Dict[str, Any]:
    """Analyze address data if present."""
    addresses = {}
    
    # Find address columns
    address_cols = [col for col in df.columns if col.lower() in ['address', 'street', 'street_address']]
    if address_cols:
        address_col = address_cols[0]
        
        # Check for missing addresses
        missing_addresses = df[address_col].isna().sum()
        addresses['missing'] = {
            'count': int(missing_addresses),
            'percent': round((missing_addresses / len(df)) * 100, 2)
        }
        
        # Check for potential address format issues (very basic check)
        if df[address_col].dtype == 'object':
            # Check for addresses that are too short (likely incomplete)
            short_addresses = (df[address_col].str.len() < 10).sum()
            addresses['potentially_incomplete'] = {
                'count': int(short_addresses),
                'percent': round((short_addresses / len(df)) * 100, 2)
            }
    
    return addresses


@task(name="Analyze Phone Numbers", tags=["data-analysis", "cpg"])
def analyze_phone_numbers(df: pd.DataFrame) -> Dict[str, Any]:
    """Analyze phone number data if present."""
    phone_numbers = {}
    
    # Find phone columns
    phone_cols = [col for col in df.columns if 'phone' in col.lower()]
    if phone_cols:
        phone_col = phone_cols[0]
        
        # Check for missing phone numbers
        missing_phones = df[phone_col].isna().sum()
        phone_numbers['missing'] = {
            'count': int(missing_phones),
            'percent': round((missing_phones / len(df)) * 100, 2)
        }
        
        # Check for potentially invalid phone numbers (basic pattern check)
        if df[phone_col].dtype == 'object':
            # This is a very basic check - would need to be refined for production
            valid_pattern = r'^\+?[0-9\-\(\)\s]{7,20}$'
            invalid_phones = (~df[phone_col].str.match(valid_pattern)).sum() - missing_phones
            phone_numbers['potentially_invalid'] = {
                'count': int(invalid_phones),
                'percent': round((invalid_phones / len(df)) * 100, 2)
            }
    
    return phone_numbers


@task(name="Analyze Temporal Data", tags=["data-analysis", "cpg"])
def analyze_temporal_data(df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    """Analyze date/time data if present."""
    temporal_data = {}
    
    # Find date columns
    date_cols = [col for col in df.columns if any(date_term in col.lower() 
                                                for date_term in ['date', 'time', 'year', 'month', 'day'])]
    
    for date_col in date_cols:
        temporal_data[date_col] = {}
        
        # Check for missing dates
        missing_dates = df[date_col].isna().sum()
        temporal_data[date_col]['missing'] = {
            'count': int(missing_dates),
            'percent': round((missing_dates / len(df)) * 100, 2)
        }
        
        # Try to convert to datetime and get min/max if possible
        try:
            if df[date_col].dtype != 'datetime64[ns]':
                # Try common date formats to avoid inference warnings
                try:
                    # First try ISO format
                    date_series = pd.to_datetime(df[date_col], format='%Y-%m-%d', errors='coerce')
                    # If too many NaT values, try with different formats
                    if date_series.isna().mean() > 0.5:
                        date_series = pd.to_datetime(df[date_col], format='%m/%d/%Y', errors='coerce')
                    if date_series.isna().mean() > 0.5:
                        date_series = pd.to_datetime(df[date_col], format='%d/%m/%Y', errors='coerce')
                    if date_series.isna().mean() > 0.5:
                        # Fall back to let pandas infer the format if all else fails
                        date_series = pd.to_datetime(df[date_col], errors='coerce')
                except Exception as e:
                    print(f"Error converting {date_col} with specific formats: {str(e)}")
                    # Fall back to let pandas infer the format if all else fails
                    date_series = pd.to_datetime(df[date_col], errors='coerce')
            else:
                date_series = df[date_col]
            
            if not date_series.isna().all():
                temporal_data[date_col]['min_date'] = date_series.min().strftime('%Y-%m-%d')
                temporal_data[date_col]['max_date'] = date_series.max().strftime('%Y-%m-%d')
                
                # Calculate date range in days
                date_range = (date_series.max() - date_series.min()).days
                temporal_data[date_col]['date_range_days'] = date_range
        except Exception as e:
            # If conversion fails, note that dates may be in an invalid format
            temporal_data[date_col]['format_issues'] = True
            print(f"Error processing temporal data for {date_col}: {str(e)}")
    
    return temporal_data


@task(name="Calculate Quality Score", tags=["data-analysis"])
def calculate_quality_score(analysis_results: Dict[str, Any]) -> Dict[str, float]:
    """Calculate overall data quality score based on analysis results."""
    quality_score = 100
    
    # Penalize for missing values
    if analysis_results.get('overall_missing_percent', 0) > 0:
        quality_score -= min(30, analysis_results['overall_missing_percent'] / 2)  # Max 30 point penalty
    
    # Penalize for duplicate rows
    if analysis_results.get('duplicate_rows', {}).get('percent', 0) > 0:
        quality_score -= min(20, analysis_results['duplicate_rows']['percent'] * 2)  # Max 20 point penalty
    
    # Penalize for location data issues if they exist
    if 'location_data' in analysis_results and 'invalid_coordinates' in analysis_results['location_data']:
        quality_score -= min(15, analysis_results['location_data']['invalid_coordinates']['percent'] / 2)  # Max 15 point penalty
    
    # Penalize for missing critical fields
    critical_fields_penalty = 0
    for field_type in ['addresses', 'business_names', 'categories', 'phone_numbers']:
        if field_type in analysis_results and 'missing' in analysis_results[field_type]:
            critical_fields_penalty += analysis_results[field_type]['missing']['percent'] / 10
    
    quality_score -= min(25, critical_fields_penalty)  # Max 25 point penalty
    
    # Ensure score is between 0 and 100
    quality_score = max(0, min(100, quality_score))
    
    # Calculate completeness score (percentage of non-null values)
    completeness = 100 - analysis_results.get('overall_missing_percent', 0)
    
    return {
        'quality_score': round(quality_score, 1),
        'completeness': round(completeness, 1)
    }


@task(name="Analyze Data", description="Coordinate data analysis tasks", tags=["data-analysis"])
def analyze_data(df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
    """
    Coordinate data analysis tasks for CPG and point-of-interest data.
    
    Args:
        df: Pandas DataFrame containing the data
        table_name: Name of the table being analyzed
        
    Returns:
        Dictionary containing analysis results
    """
    # Initialize results dictionary
    analysis = {}
    
    # Execute analysis tasks
    basic_stats = calculate_basic_statistics(df)
    analysis.update(basic_stats)
    
    # Missing values analysis
    missing_values_analysis = analyze_missing_values(df)
    analysis['missing_values'] = missing_values_analysis['column_missing']
    analysis['overall_missing_percent'] = missing_values_analysis['overall_missing_percent']
    
    # Generate missing values visualization
    try:
        plot_path = plot_missing_values(analysis['missing_values'], 
                                       title=f"Missing Values in {table_name}")
        analysis['visualizations'] = analysis.get('visualizations', {})
        analysis['visualizations']['missing_values'] = plot_path
    except Exception as e:
        print(f"Error generating missing values visualization: {str(e)}")
    
    # Duplicate analysis
    analysis['duplicate_rows'] = analyze_duplicates(df)
    
    # CPG-specific analyses
    analysis['location_data'] = analyze_location_data(df)
    analysis['business_names'] = analyze_business_names(df)
    
    # Category analysis
    category_analysis = analyze_categories(df, table_name)
    analysis['categories'] = category_analysis['categories']
    if 'visualizations' in category_analysis and category_analysis['visualizations']:
        analysis['visualizations'] = analysis.get('visualizations', {})
        analysis['visualizations'].update(category_analysis['visualizations'])
    
    # Address and phone analysis
    analysis['addresses'] = analyze_addresses(df)
    analysis['phone_numbers'] = analyze_phone_numbers(df)
    
    # Temporal data analysis
    analysis['temporal_data'] = analyze_temporal_data(df)
    
    # Calculate quality scores
    quality_scores = calculate_quality_score(analysis)
    analysis.update(quality_scores)
    
    return analysis
