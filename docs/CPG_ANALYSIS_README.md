# CPG Data Analysis Tools

This module provides tools for analyzing Consumer Packaged Goods (CPG) data stored in a DuckDB database. The analysis focuses on key business areas for CPG companies:

1. **Store/Retailer Targeting & Distribution Planning**
2. **Market Analysis & Competitive Intelligence**
3. **Sales Territory Management**
4. **Data Quality Assessment**

## Getting Started

### Prerequisites

- Python 3.7 or higher
- DuckDB
- pandas
- matplotlib and seaborn (for visualizations)
- prefect
- streamlit
- altair
- duckdb
- numpy

### Installation

These tools are designed to work with your existing DuckDB database. Simply place the Python files in your project directory.

## Usage

### Basic Usage

```python
import cpg_analysis

# Connect to your DuckDB database
conn, table_name = cpg_analysis.connect_db('my_db')

# Run a specific analysis
chains = cpg_analysis.identify_chain_store_targets(conn)
print(f"Found {len(chains)} chains suitable for distribution deals")

# Run all analyses at once
all_results = cpg_analysis.run_all_analyses(conn)
```

### Command Line Usage

You can also run analyses directly from the command line:

```bash
# Run all analyses
python cpg_analysis.py --db my_db

# Run a specific analysis and save results
python cpg_analysis.py --db my_db --analysis chain_targets --output results/

# Specify a different table name
python cpg_analysis.py --db my_db --table my_location_table
```

### Example Script

A comprehensive example script is provided in `cpg_example.py`:

```bash
python cpg_example.py
```

This script demonstrates how to:
- Connect to your database
- Run multiple analyses
- Create visualizations of the results
- Save analysis results to CSV files

## Available Analyses

### Store/Retailer Targeting & Distribution Planning

- `get_active_distribution_points()`: Identifies open retail and grocery locations
- `analyze_delivery_windows()`: Analyzes business hours for optimal delivery scheduling
- `identify_chain_store_targets()`: Finds chains with multiple locations for efficient distribution deals
- `find_distribution_gaps()`: Identifies cities with limited retail coverage

### Market Analysis & Competitive Intelligence

- `analyze_retail_segments()`: Analyzes the distribution of retail categories
- `analyze_competitive_density()`: Maps retail density by postal code
- `compare_customer_engagement()`: Compares customer engagement metrics across retail categories

### Territory Management

- `analyze_territory_coverage()`: Maps retail distribution by city for sales territory planning
- `analyze_geographic_clusters()`: Identifies retail clusters for efficient territory visits

### Data Quality Assessment

- `assess_critical_data_completeness()`: Assess completeness of critical fields for CPG operations
- `assess_chain_data_quality()`: Assess data quality for important CPG retail chains

## Example Output

The analyses will generate insights such as:

1. **Chain Store Analysis**: Top retail chains for distribution deals, with location counts and geographical spread
2. **Territory Analysis**: Retail distribution by city, showing concentration of retail, grocery, and chain stores
3. **Data Quality Assessment**: Completeness metrics for critical fields like addresses, business hours, and websites

## Data Schema

The module expects a DuckDB table with the following key columns:
- `dataplor_id`: Unique identifier for each location
- `name`: Business name
- `main_category`, `sub_category`: Business categorization
- `chain_id`, `chain_name`: Chain affiliation
- `address`, `city`, `state`, `postal_code`, `latitude`, `longitude`: Location data
- `website`: Business website
- `monday_open` through `sunday_close`: Business hours
- `open_closed_status`: Current operating status
- `data_quality_confidence_score`: Data quality indicator
- `popularity_score`, `sentiment_score`, `dwell_time`: Customer engagement metrics

## Extending the Module

You can extend the module by adding new analysis functions following this pattern:

```python
def my_new_analysis(
    conn: duckdb.DuckDBPyConnection, 
    table_name: str = 'boisedemodatasampleaug',
    param1: int = 10
) -> pd.DataFrame:
    """
    Description of your analysis.
    
    Args:
        conn: DuckDB database connection
        table_name: Name of the table containing location data
        param1: Your custom parameter
        
    Returns:
        DataFrame with analysis results
    """
    query = f"""
    SELECT 
        column1,
        column2,
        COUNT(*) as count
    FROM {table_name}
    WHERE some_condition = {param1}
    GROUP BY column1, column2
    """
    
    return conn.execute(query).df()
```

Then add your function to `run_all_analyses()` to include it in comprehensive analyses.

## License

Internal use only.

## Acknowledgments

- Based on the "Most Valuable Columns for CPG Client" SQL queries
- Uses DuckDB for efficient query execution


## Recommendations for Simplifying and Fixing Errors

### 1. Data Loading & Connection Improvements

**Current Issues**:
- The database connection management could be more robust
- Error handling during data loading is basic
- No validation of loaded data before analysis

**Recommendations**:
1. **Enhance Connection Pooling**:
   - Improve the connection management to handle timeouts and reconnections
   - Add proper resource cleanup using context managers consistently
   - Implement connection health checks

2. **Add Data Validation During Loading**:
   - Implement schema validation when loading tables
   - Add pre-checks for data types and basic structural requirements
   - Provide immediate feedback on data quality issues during loading

3. **Implement Better Error Recovery**:
   - Add retry mechanisms for transient database errors
   - Improve error messages with actionable information
   - Implement partial loading capabilities for large datasets

### 2. Analysis Pipeline Optimization

**Current Issues**:
- The analysis can be slow for large datasets
- Some analysis functions perform redundant calculations
- Error detection thresholds are hardcoded

**Recommendations**:
1. **Refactor Analysis Pipeline**:
   - Split analysis into smaller, more focused tasks
   - Implement progressive loading for large datasets
   - Add caching for analysis results

2. **Make Thresholds Configurable**:
   - Move hardcoded thresholds to a configuration file
   - Allow customization based on business needs
   - Add category-specific thresholds

3. **Improve Performance**:
   - Optimize expensive operations like duplicate detection
   - Use sampling for initial analysis of large datasets
   - Push more computation to the database layer

### 3. Data Cleaning Enhancements

**Current Issues**:
- Cleaning operations are applied sequentially
- No way to track lineage of cleaning operations
- Limited validation of cleaned data

**Recommendations**:
1. **Implement Cleaning Workflows**:
   - Create a workflow system for chaining cleaning operations
   - Add dependency management for cleaning steps
   - Implement rollback capabilities for failed cleaning operations

2. **Add Data Lineage Tracking**:
   - Record all changes made during cleaning
   - Create audit trails for data transformations
   - Enable before/after comparisons

3. **Enhance Validation**:
   - Add post-cleaning validation checks
   - Implement business rule validation
   - Add statistical checks to ensure cleaning didn't introduce new issues

### 4. User Interface Simplification

**Current Issues**:
- User interface can be overwhelming with many options
- Navigation between sections isn't always intuitive
- Visualizations could be more informative

**Recommendations**:
1. **Streamline the UI**:
   - Create a wizard-like interface for common tasks
   - Group related functionality more logically
   - Implement progressive disclosure for advanced options

2. **Improve Visualizations**:
   - Create more interactive and informative visualizations
   - Add drill-down capabilities for exploring issues
   - Implement comparative views for before/after cleaning

3. **Add User Guidance**:
   - Provide contextual help and tooltips
   - Add guided tours for new users
   - Implement intelligent recommendations based on data characteristics

### 5. Architecture Improvements

**Current Issues**:
- Monolithic design makes updates and testing difficult
- Limited separation between UI and business logic
- Minimal error handling in some components

**Recommendations**:
1. **Modularize the Architecture**:
   - Separate the UI layer from business logic
   - Create clear API boundaries between components
   - Implement a plugin system for extensibility

2. **Improve Error Handling**:
   - Add comprehensive error handling throughout the codebase
   - Implement structured logging for better diagnostics
   - Add monitoring for critical operations

3. **Enhance Testing**:
   - Add unit tests for core functionality
   - Implement integration tests for key workflows
   - Add data quality regression tests
