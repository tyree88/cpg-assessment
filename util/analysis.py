import pandas as pd

def analyze_data(df, table_name):
    """
    Perform comprehensive data analysis with a focus on CPG and point-of-interest data
    
    Args:
        df: Pandas DataFrame containing the data
        table_name: Name of the table being analyzed
        
    Returns:
        Dictionary containing analysis results
    """
    analysis = {}
    
    # Basic statistics
    analysis['row_count'] = len(df)
    analysis['column_count'] = len(df.columns)
    
    # Column types
    analysis['column_types'] = {}
    for col in df.columns:
        analysis['column_types'][col] = str(df[col].dtype)
    
    # Missing values
    analysis['missing_values'] = {}
    for col in df.columns:
        missing_count = df[col].isna().sum()
        missing_percent = (missing_count / len(df)) * 100
        analysis['missing_values'][col] = {
            'count': int(missing_count),
            'percent': round(missing_percent, 2)
        }
    
    # Overall missing data percentage
    total_cells = len(df) * len(df.columns)
    total_missing = sum(info['count'] for info in analysis['missing_values'].values())
    analysis['overall_missing_percent'] = round((total_missing / total_cells) * 100, 2)
    
    # Duplicate rows
    duplicate_count = df.duplicated().sum()
    analysis['duplicate_rows'] = {
        'count': int(duplicate_count),
        'percent': round((duplicate_count / len(df)) * 100, 2)
    }
    
    # CPG-specific analysis
    
    # Location data analysis (if latitude/longitude columns exist)
    analysis['location_data'] = {}
    lat_cols = [col for col in df.columns if col.lower() in ['latitude', 'lat']]
    lng_cols = [col for col in df.columns if col.lower() in ['longitude', 'long', 'lng']]
    
    if lat_cols and lng_cols:
        lat_col = lat_cols[0]
        lng_col = lng_cols[0]
        
        # Check for valid coordinates
        valid_lat = df[lat_col].between(-90, 90, inclusive='both')
        valid_lng = df[lng_col].between(-180, 180, inclusive='both')
        
        analysis['location_data']['valid_coordinates'] = {
            'count': int(valid_lat.sum() & valid_lng.sum()),
            'percent': round(((valid_lat.sum() & valid_lng.sum()) / len(df)) * 100, 2)
        }
        
        analysis['location_data']['invalid_coordinates'] = {
            'count': int(len(df) - (valid_lat.sum() & valid_lng.sum())),
            'percent': round(((len(df) - (valid_lat.sum() & valid_lng.sum())) / len(df)) * 100, 2)
        }
        
        # Check for null coordinates
        null_lat = df[lat_col].isna()
        null_lng = df[lng_col].isna()
        
        analysis['location_data']['null_coordinates'] = {
            'count': int(null_lat.sum() | null_lng.sum()),
            'percent': round(((null_lat.sum() | null_lng.sum()) / len(df)) * 100, 2)
        }
    
    # Business name analysis (if name column exists)
    name_cols = [col for col in df.columns if col.lower() in ['name', 'business_name', 'store_name', 'poi_name']]
    if name_cols:
        name_col = name_cols[0]
        analysis['business_names'] = {}
        
        # Check for missing business names
        missing_names = df[name_col].isna().sum()
        analysis['business_names']['missing'] = {
            'count': int(missing_names),
            'percent': round((missing_names / len(df)) * 100, 2)
        }
        
        # Check for duplicate business names
        duplicate_names = df[name_col].duplicated().sum()
        analysis['business_names']['duplicates'] = {
            'count': int(duplicate_names),
            'percent': round((duplicate_names / len(df)) * 100, 2)
        }
    
    # Category/classification analysis (if category column exists)
    category_cols = [col for col in df.columns if col.lower() in ['category', 'categories', 'classification', 'type', 'business_type']]
    if category_cols:
        category_col = category_cols[0]
        analysis['categories'] = {}
        
        # Check for missing categories
        missing_categories = df[category_col].isna().sum()
        analysis['categories']['missing'] = {
            'count': int(missing_categories),
            'percent': round((missing_categories / len(df)) * 100, 2)
        }
        
        # Get category distribution
        category_counts = df[category_col].value_counts().to_dict()
        analysis['categories']['distribution'] = {
            str(k): int(v) for k, v in category_counts.items() if pd.notna(k)
        }
    
    # Address analysis (if address columns exist)
    address_cols = [col for col in df.columns if col.lower() in ['address', 'street', 'street_address']]
    if address_cols:
        address_col = address_cols[0]
        analysis['addresses'] = {}
        
        # Check for missing addresses
        missing_addresses = df[address_col].isna().sum()
        analysis['addresses']['missing'] = {
            'count': int(missing_addresses),
            'percent': round((missing_addresses / len(df)) * 100, 2)
        }
        
        # Check for potential address format issues (very basic check)
        if df[address_col].dtype == 'object':
            # Check for addresses that are too short (likely incomplete)
            short_addresses = (df[address_col].str.len() < 10).sum()
            analysis['addresses']['potentially_incomplete'] = {
                'count': int(short_addresses),
                'percent': round((short_addresses / len(df)) * 100, 2)
            }
    
    # Phone number analysis (if phone column exists)
    phone_cols = [col for col in df.columns if 'phone' in col.lower()]
    if phone_cols:
        phone_col = phone_cols[0]
        analysis['phone_numbers'] = {}
        
        # Check for missing phone numbers
        missing_phones = df[phone_col].isna().sum()
        analysis['phone_numbers']['missing'] = {
            'count': int(missing_phones),
            'percent': round((missing_phones / len(df)) * 100, 2)
        }
        
        # Check for potentially invalid phone numbers (basic pattern check)
        if df[phone_col].dtype == 'object':
            # This is a very basic check - would need to be refined for production
            valid_pattern = r'^\+?[0-9\-\(\)\s]{7,20}$'
            invalid_phones = (~df[phone_col].str.match(valid_pattern)).sum() - missing_phones
            analysis['phone_numbers']['potentially_invalid'] = {
                'count': int(invalid_phones),
                'percent': round((invalid_phones / len(df)) * 100, 2)
            }
    
    # Date/time analysis (if date columns exist)
    date_cols = [col for col in df.columns if any(date_term in col.lower() for date_term in ['date', 'time', 'year', 'month', 'day'])]
    if date_cols:
        analysis['temporal_data'] = {}
        
        for date_col in date_cols:
            analysis['temporal_data'][date_col] = {}
            
            # Check for missing dates
            missing_dates = df[date_col].isna().sum()
            analysis['temporal_data'][date_col]['missing'] = {
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
                    except Exception:
                        # Fall back to let pandas infer the format if all else fails
                        date_series = pd.to_datetime(df[date_col], errors='coerce')
                else:
                    date_series = df[date_col]
                
                if not date_series.isna().all():
                    analysis['temporal_data'][date_col]['min_date'] = date_series.min().strftime('%Y-%m-%d')
                    analysis['temporal_data'][date_col]['max_date'] = date_series.max().strftime('%Y-%m-%d')
                    
                    # Calculate date range in days
                    date_range = (date_series.max() - date_series.min()).days
                    analysis['temporal_data'][date_col]['date_range_days'] = date_range
            except Exception:
                # If conversion fails, note that dates may be in an invalid format
                analysis['temporal_data'][date_col]['format_issues'] = True
    
    return analysis

def identify_data_quality_issues(df, analysis, table_name):
    """
    Identify data quality issues with a focus on CPG and point-of-interest data
    
    Args:
        df: Pandas DataFrame containing the data
        analysis: Dictionary containing analysis results
        table_name: Name of the table being analyzed
        
    Returns:
        Dictionary containing identified issues
    """
    issues = {}
    
    # Critical issues (high priority)
    issues['critical'] = []
    
    # Check for high percentage of missing values
    high_missing_cols = [col for col, info in analysis['missing_values'].items() 
                        if info['percent'] > 20]  # Threshold for critical missing data
    
    if high_missing_cols:
        issues['critical'].append({
            'type': 'high_missing_values',
            'description': f"High percentage of missing values in {len(high_missing_cols)} columns",
            'affected_columns': high_missing_cols,
            'details': {col: analysis['missing_values'][col] for col in high_missing_cols}
        })
    
    # Check for high percentage of duplicate rows
    if analysis['duplicate_rows']['percent'] > 5:  # Threshold for critical duplicate data
        issues['critical'].append({
            'type': 'high_duplicate_rows',
            'description': f"High percentage of duplicate rows: {analysis['duplicate_rows']['percent']}%",
            'details': analysis['duplicate_rows']
        })
    
    # Check for location data issues
    if 'location_data' in analysis and 'invalid_coordinates' in analysis['location_data']:
        if analysis['location_data']['invalid_coordinates']['percent'] > 10:
            issues['critical'].append({
                'type': 'invalid_coordinates',
                'description': f"High percentage of invalid coordinates: {analysis['location_data']['invalid_coordinates']['percent']}%",
                'details': analysis['location_data']['invalid_coordinates']
            })
    
    # Check for business name issues
    if 'business_names' in analysis and 'missing' in analysis['business_names']:
        if analysis['business_names']['missing']['percent'] > 10:
            issues['critical'].append({
                'type': 'missing_business_names',
                'description': f"High percentage of missing business names: {analysis['business_names']['missing']['percent']}%",
                'details': analysis['business_names']['missing']
            })
    
    # Check for category/classification issues
    if 'categories' in analysis and 'missing' in analysis['categories']:
        if analysis['categories']['missing']['percent'] > 15:
            issues['critical'].append({
                'type': 'missing_categories',
                'description': f"High percentage of missing categories: {analysis['categories']['missing']['percent']}%",
                'details': analysis['categories']['missing']
            })
    
    # Warning issues (medium priority)
    issues['warnings'] = []
    
    # Check for columns with moderate missing values
    moderate_missing_cols = [col for col, info in analysis['missing_values'].items() 
                           if 5 < info['percent'] <= 20]  # Threshold for warning missing data
    
    if moderate_missing_cols:
        issues['warnings'].append({
            'type': 'moderate_missing_values',
            'description': f"Moderate percentage of missing values in {len(moderate_missing_cols)} columns",
            'affected_columns': moderate_missing_cols,
            'details': {col: analysis['missing_values'][col] for col in moderate_missing_cols}
        })
    
    # Check for moderate percentage of duplicate rows
    if 1 < analysis['duplicate_rows']['percent'] <= 5:  # Threshold for warning duplicate data
        issues['warnings'].append({
            'type': 'moderate_duplicate_rows',
            'description': f"Moderate percentage of duplicate rows: {analysis['duplicate_rows']['percent']}%",
            'details': analysis['duplicate_rows']
        })
    
    # Check for address issues
    if 'addresses' in analysis and 'potentially_incomplete' in analysis['addresses']:
        if analysis['addresses']['potentially_incomplete']['percent'] > 5:
            issues['warnings'].append({
                'type': 'incomplete_addresses',
                'description': f"Potentially incomplete addresses: {analysis['addresses']['potentially_incomplete']['percent']}%",
                'details': analysis['addresses']['potentially_incomplete']
            })
    
    # Check for phone number issues
    if 'phone_numbers' in analysis and 'potentially_invalid' in analysis['phone_numbers']:
        if analysis['phone_numbers']['potentially_invalid']['percent'] > 5:
            issues['warnings'].append({
                'type': 'invalid_phone_numbers',
                'description': f"Potentially invalid phone numbers: {analysis['phone_numbers']['potentially_invalid']['percent']}%",
                'details': analysis['phone_numbers']['potentially_invalid']
            })
    
    # Information issues (low priority)
    issues['info'] = []
    
    # Check for columns with low missing values
    low_missing_cols = [col for col, info in analysis['missing_values'].items() 
                       if 0 < info['percent'] <= 5]  # Threshold for info missing data
    
    if low_missing_cols:
        issues['info'].append({
            'type': 'low_missing_values',
            'description': f"Low percentage of missing values in {len(low_missing_cols)} columns",
            'affected_columns': low_missing_cols,
            'details': {col: analysis['missing_values'][col] for col in low_missing_cols}
        })
    
    # Check for low percentage of duplicate rows
    if 0 < analysis['duplicate_rows']['percent'] <= 1:  # Threshold for info duplicate data
        issues['info'].append({
            'type': 'low_duplicate_rows',
            'description': f"Low percentage of duplicate rows: {analysis['duplicate_rows']['percent']}%",
            'details': analysis['duplicate_rows']
        })
    
    # Check for temporal data issues
    if 'temporal_data' in analysis:
        for date_col, date_info in analysis['temporal_data'].items():
            if 'format_issues' in date_info and date_info['format_issues']:
                issues['info'].append({
                    'type': 'date_format_issues',
                    'description': f"Potential date format issues in column: {date_col}",
                    'affected_columns': [date_col]
                })
    
    # Calculate overall data quality score (simplified)
    # Score from 0-100, where 100 is perfect quality
    # This is a simplified scoring mechanism and should be refined for production
    
    # Start with perfect score and subtract for issues
    quality_score = 100
    
    # Critical issues have high impact
    quality_score -= len(issues['critical']) * 15
    
    # Warning issues have medium impact
    quality_score -= len(issues['warnings']) * 5
    
    # Info issues have low impact
    quality_score -= len(issues['info']) * 1
    
    # Adjust for overall missing data percentage
    quality_score -= analysis['overall_missing_percent'] * 0.5
    
    # Ensure score doesn't go below 0
    quality_score = max(0, quality_score)
    
    # Round to nearest integer
    quality_score = round(quality_score)
    
    issues['quality_score'] = quality_score
    
    return issues
