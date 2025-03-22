# DataPlor - Data Quality Assessment Tool

A comprehensive data quality assessment tool designed for analyzing point-of-interest (POI) and location data. Built with Streamlit and DuckDB, this tool provides an interactive interface for data analysis, quality assessment, and cleaning, with specific features for CPG (Consumer Packaged Goods) data.

## 🚀 Quick Start

1. Clone the repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```
3. Run the application:
```bash
streamlit run Home.py
```

## 📁 Project Structure

```
dataplor/
├── Home.py                 # Main application entry point
├── components/            # UI Components
│   ├── data_analysis.py   # Data analysis components
│   ├── data_cleaning.py   # Data cleaning interface
│   ├── data_report.py     # Report generation
│   ├── ui_helpers.py      # Reusable UI utilities
│   ├── ui_components.py   # Common UI elements
│   └── cpg_queries.py     # CPG-specific analysis
├── util/                 # Utility Functions
│   ├── analysis.py        # Core analysis logic
│   ├── cleaning.py        # Data cleaning operations
│   ├── database.py        # DuckDB connection handling
│   └── styles.py          # Custom CSS styles
└── static/              # Static Assets
    └── architecture.jpg   # System architecture diagram
```

## 🎯 Core Features

### 1. Data Analysis
- Load and analyze data from DuckDB
- Comprehensive data profiling
- Quality metrics calculation
- Missing value analysis
- Duplicate detection

### 2. Data Cleaning
- Automated cleaning recommendations
- Missing value handling
- Duplicate record resolution
- Format standardization
- Data validation

### 3. CPG-Specific Features
- Business category analysis
- Location clustering
- Market coverage assessment
- Territory analysis

## 🔍 Key Components

### Home.py
The main application file that:
- Initializes the Streamlit interface
- Manages application state
- Coordinates component interactions
- Handles data loading and processing

### Components
- `data_analysis.py`: Data profiling and analysis visualizations
- `data_cleaning.py`: Interactive cleaning interface
- `data_report.py`: Report generation functionality
- `ui_helpers.py`: Common UI utilities
- `cpg_queries.py`: CPG-specific analysis tools

### Utilities
- `analysis.py`: Core data analysis functions
- `cleaning.py`: Data cleaning operations
- `database.py`: Database connection management
- `styles.py`: Custom styling definitions

## 💡 Usage Guide

1. **Data Loading**
   - Select your data source from the dropdown
   - Click "Load Data" to begin analysis

2. **Data Analysis**
   - View data quality metrics
   - Explore column-level statistics
   - Identify quality issues

3. **Data Cleaning**
   - Review cleaning recommendations
   - Apply automated fixes
   - Validate results

4. **CPG Analysis**
   - Analyze business categories
   - Assess market coverage
   - Generate insights

5. **Report Generation**
   - Create comprehensive reports
   - Export findings
   - Track improvements

## 🛠 Development

### Adding New Features
1. Create component in appropriate directory
2. Update Home.py to include new component
3. Add any required utility functions
4. Update documentation

### Styling Guidelines
- Use Streamlit components for consistency
- Follow existing naming conventions
- Include docstrings and comments
- Add type hints for better maintainability

## 📊 Data Quality Framework

The application implements a three-layer architecture:
1. **Ingestion**: Data loading and validation
2. **Processing**: Analysis and cleaning
3. **Distribution**: Reporting and exports

## 📈 Key Queries & Analysis

### Data Quality Assessment
```sql
-- Example of completeness check
SELECT 
    column_name,
    COUNT(*) as total_records,
    COUNT(*) - COUNT(column_name) as missing_count,
    ROUND(100.0 * (COUNT(*) - COUNT(column_name)) / COUNT(*), 2) as missing_percentage
FROM your_table
GROUP BY column_name;
```
Analyzes data completeness, identifying missing values and their distribution across different fields.

### Duplicate Detection
```sql
-- Example of duplicate detection
WITH duplicates AS (
    SELECT 
        business_name,
        address,
        COUNT(*) as occurrence_count
    FROM locations
    GROUP BY business_name, address
    HAVING COUNT(*) > 1
)
```
Identifies potential duplicate records based on business rules and similarity metrics.

### Business Category Analysis
```sql
-- Example of category distribution
SELECT 
    category,
    COUNT(*) as location_count,
    COUNT(DISTINCT parent_category) as parent_categories
FROM business_categories
GROUP BY category
ORDER BY location_count DESC;
```
Analyzes the distribution and hierarchy of business categories.

### Market Coverage
```sql
-- Example of geographic coverage
SELECT 
    postal_code,
    COUNT(*) as location_count,
    COUNT(DISTINCT category) as category_diversity
FROM locations
GROUP BY postal_code;
```
Assesses market penetration and coverage across different geographic areas.

### Data Confidence Scoring
```sql
-- Example of confidence scoring
SELECT 
    CASE 
        WHEN confidence_score >= 0.9 THEN 'High'
        WHEN confidence_score >= 0.7 THEN 'Medium'
        ELSE 'Low'
    END as confidence_level,
    COUNT(*) as record_count
FROM locations
GROUP BY confidence_level;
```
Evaluates data quality confidence scores and their distribution.

### Operational Analysis
```sql
-- Example of hours coverage
SELECT 
    day_of_week,
    COUNT(*) as total_locations,
    COUNT(opening_hours) as hours_available,
    ROUND(100.0 * COUNT(opening_hours) / COUNT(*), 2) as completion_rate
FROM operational_hours
GROUP BY day_of_week;
```
Analyzes operational data completeness and patterns.

### Chain Store Analysis
```sql
-- Example of chain identification
SELECT 
    business_name,
    COUNT(*) as location_count,
    COUNT(DISTINCT postal_code) as postal_codes_covered
FROM locations
GROUP BY business_name
HAVING COUNT(*) >= 5;
```
Identifies and analyzes chain store patterns and distribution.

### Data Standardization Checks
```sql
-- Example of format consistency
SELECT 
    postal_code_format,
    COUNT(*) as occurrence_count
FROM (
    SELECT CASE 
        WHEN postal_code ~ '^\d{5}$' THEN '5-digit'
        WHEN postal_code ~ '^\d{5}-\d{4}$' THEN '9-digit'
        ELSE 'other'
    END as postal_code_format
    FROM locations
)
GROUP BY postal_code_format;
```
Validates data format consistency and standardization.

Each query type serves specific analytical purposes:

1. **Quality Assessment**
   - Completeness analysis
   - Format validation
   - Consistency checks
   - Data type verification

2. **Business Intelligence**
   - Chain store identification
   - Market coverage analysis
   - Category distribution
   - Operational patterns

3. **Data Cleaning**
   - Duplicate detection
   - Format standardization
   - Missing value identification
   - Anomaly detection

4. **Operational Insights**
   - Hours coverage
   - Geographic distribution
   - Category relationships
   - Chain store patterns

These queries form the foundation of the data quality assessment and provide actionable insights for data improvement.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## 📝 License

[MIT License](LICENSE)

## 🆘 Support

For issues and questions, please [create an issue](https://github.com/yourusername/dataplor/issues) in the repository.
