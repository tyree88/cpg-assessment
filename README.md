# Dataplor - Data Quality Assessment & Cleaning Tool

A comprehensive data analysis and cleaning tool built for point-of-interest data, company location data, and metrics stored in DuckDB. This tool provides an interactive interface for analyzing data quality, identifying issues, and cleaning data with a focus on CPG (Consumer Packaged Goods) client data.

## Features

- **Data Loading**: Load data from CSV or Excel files into DuckDB for analysis
- **Data Analysis**: Comprehensive data profiling including missing values, data types, and quality metrics
- **Data Quality Assessment**: Automatic identification of data quality issues such as:
  - Missing values
  - Duplicate records
  - Inconsistent data types
  - Outliers
  - JSON data stored as strings
- **Data Cleaning**: Interactive interface for cleaning data with options for:
  - Handling missing values (mean, median, mode, custom value)
  - Removing duplicates
  - Converting data types
  - Handling outliers
  - Parsing JSON columns
- **SQL Explorer**: Run custom SQL queries against your data

## Installation

1. Clone this repository
2. Install dependencies:

```bash
pip install -e .
```

## Usage

Run the Streamlit app:

```bash
streamlit run pages/Dataplor.py
```

The application will open in your default web browser with the following pages:

1. **Load Data**: Upload your data or use the sample dataset
2. **Analyze Data**: View comprehensive data analysis and quality metrics
3. **Clean Data**: Apply data cleaning operations based on recommendations
4. **SQL Explorer**: Run custom SQL queries against your data

## Data Flow

1. Data is loaded from CSV/Excel into a pandas DataFrame
2. The DataFrame is analyzed for quality issues
3. The data is loaded into DuckDB for SQL-based analysis
4. Cleaning recommendations are generated based on identified issues
5. User selects cleaning operations to perform
6. Cleaned data is saved back to DuckDB and available for download

## Requirements

- Python 3.12+
- Streamlit 1.30.0+
- pandas 2.0.0+
- DuckDB 0.9.0+
- numpy 1.24.0+
- openpyxl 3.1.0+ (for Excel support)
