# DataPlor - Data Quality Assessment & Cleaning Tool

A comprehensive data analysis and cleaning tool built for point-of-interest (POI) data, company location data, and metrics stored in DuckDB. This tool provides an interactive interface for analyzing data quality, identifying issues, and cleaning data with a focus on CPG (Consumer Packaged Goods) client data.

## Features

### Core Functionality

- **Data Loading**: Load data from DuckDB tables for analysis
- **Data Analysis**: Comprehensive data profiling including missing values, data types, and quality metrics
- **Data Quality Assessment**: Automatic identification of data quality issues
- **Data Cleaning**: Interactive interface for cleaning data
- **CPG Analysis**: Specialized tools for CPG data analysis and quality metrics

### Data Quality Assessment

The application automatically identifies issues such as:

- Missing values in critical fields
- Duplicate records
- Inconsistent data types
- Outliers
- Data quality confidence scores
- Chain store data consistency

### Data Cleaning Capabilities

Interactive interface for cleaning data with options for:

- Handling missing values (mean, median, mode, custom value)
- Removing duplicates based on configurable criteria
- Converting data types
- Handling outliers
- Basic data transformations

### CPG-Specific Analysis

- Distribution quality metrics
- Chain store analysis
- Geographic coverage assessment
- Territory management insights
- Competitive density analysis
- Delivery window optimization

## Installation

1. Clone this repository
2. Install dependencies:

```bash
pip install -r requirements.txt
```

## Usage

Run the Streamlit app:

```bash
streamlit run Home.py
```

The application will open in your default web browser with the following tabs:

1. **Overview**: Get started with the application
2. **Data Analysis**: View comprehensive data analysis and quality metrics
3. **Data Cleaning**: Apply data cleaning operations based on recommendations
4. **CPG Analysis**: Access specialized CPG data quality metrics and analysis queries
5. **Report**: Generate and view data quality reports

## Data Flow

1. Data is loaded from DuckDB tables
2. The data is analyzed for quality issues
3. Cleaning recommendations are generated based on identified issues
4. User selects cleaning operations to perform
5. Cleaned data is saved back to DuckDB
6. CPG-specific analyses can be performed on the cleaned data

## Requirements

- Python 3.9+
- Streamlit 1.24.0+
- pandas 1.5.0+
- DuckDB 0.8.0+
- numpy 1.23.0+
- matplotlib 3.6.0+
- seaborn 0.12.0+

## Future Improvements

### User Interface Enhancements

- Add dark mode support
- Implement responsive design for mobile devices
- Create custom visualization components for CPG-specific metrics
- Add user authentication and role-based access control

### Data Processing Capabilities

- Implement machine learning-based data quality scoring
- Add support for geospatial data visualization and analysis
- Develop automated data quality monitoring with alerts
- Create a scheduling system for periodic data quality checks

### Integration Opportunities

- Connect with external data sources for enrichment
- Implement API endpoints for programmatic access
- Add export functionality to various formats (CSV, Excel, JSON)
- Integrate with data catalogs and metadata repositories

### Architecture Improvements

- Containerize the application with Docker
- Implement a microservices architecture for scalability
- Add support for distributed processing of large datasets
- Create a plugin system for custom data quality rules

### Prefect Workflow Integration

#### Architecture Design

The Prefect integration follows a data-centric architecture where the data loading component serves as the central flow, with all other functionality connected as tasks within this flow:

```ascii
Data Loading Flow
    ├── Data Quality Assessment Tasks
    │   ├── Analyze Data
    │   └── Identify Issues
    ├── Data Cleaning Tasks
    │   ├── Generate Recommendations
    │   └── Apply Cleaning Operations
    └── CPG Analysis Tasks
        ├── Chain Store Analysis
        ├── Territory Coverage
        └── Retail Segment Analysis
```

This architecture provides several benefits:

- **Single Source of Truth**: The data loading component controls all data access, ensuring consistency
- **Clear Dependencies**: All tasks depend on the data loading flow, creating a clear execution path
- **Simplified Orchestration**: Prefect manages task dependencies and execution order automatically
- **Improved Observability**: Centralized flow makes it easier to monitor the entire data pipeline
- **Scalability**: Tasks can be executed in parallel where dependencies allow

#### Planned Enhancements

- Separate UI from processing logic for better maintainability
- Add scheduling capabilities for regular data quality checks
- Implement notification systems for data quality alerts
- Add caching for performance optimization of expensive operations
- Implement retries for resilient data operations
- Create deployment pipelines for new models and analysis methods

## How dbt Improves Data Quality for the CPG Client

The dbt models I outlined provide a systematic approach to improving data quality for your CPG client. Here's a high-level overview of how these transformations address the specific issues identified in your assessment:

### 1. Addressing Data Completeness

The models systematically identify and track missing data across critical business attributes. By creating transparent metrics through the `data_quality_metrics` model, you gain visibility into exactly where the gaps are (9.3% missing address data, 27.2% missing website information, etc.). This allows you to prioritize data collection efforts and measure improvements over time.

### 2. Eliminating Duplicate Records

The `int_deduplicated_locations` model implements a sophisticated deduplication strategy that:

- Identifies the 132 potential duplicates using business rules
- Preserves the highest-quality record from each duplicate set
- Maintains a record of which entities were duplicates
- Prevents duplicate data from skewing analytics and business decisions

### 3. Improving Data Confidence

The standardization models systematically improve the overall confidence scores by:

- Applying consistent formatting rules to addresses, postal codes, and other fields
- Normalizing category hierarchies to ensure consistent classification
- Standardizing business hours formats for better operational analysis
- Tracking confidence score distributions to identify problematic data segments

### 4. Filling Operational Data Gaps

The `operational_data_analysis` model specifically addresses the significant missing hours data (38.3% missing Monday hours, 76.7% missing Sunday hours) by:

- Tracking completeness by business category and day of week
- Identifying patterns in missing data to target collection efforts
- Providing visibility into operational data quality by retail segment

### 5. Resolving Category Inconsistencies

The `category_consistency` model tackles the misalignment between industry codes and business categories by:

- Identifying uncommon or potentially incorrect category combinations
- Establishing valid hierarchy relationships between main and sub-categories
- Creating tests to enforce category taxonomy rules
- Providing documentation of the expected category relationships

### 6. Enabling Ongoing Quality Monitoring

Perhaps most importantly, these models create a sustainable framework for ongoing data governance by:

- Implementing automated tests that run with each data refresh
- Tracking quality metrics over time to identify degradation
- Establishing clear data standards through documentation
- Creating a foundation for dashboard-based quality monitoring

### 7. Business Impact

The ultimate value of these dbt models is that they transform raw, inconsistent data into a reliable foundation for business decisions by:

- Increasing confidence in market analysis through more accurate store counts
- Improving operational efficiency with standardized hours data
- Enabling more precise targeting of high-value retail locations
- Creating consistent category hierarchies for better segment analysis
- Establishing a single source of truth for location data

This structured, code-based approach means that data quality improvements are automated, reproducible, and transparent—ensuring that the CPG client can make business decisions based on data they can trust.
