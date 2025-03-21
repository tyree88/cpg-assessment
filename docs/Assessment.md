# CPG Data Quality Assessment

## Overview

This repository contains the results of a comprehensive data quality assessment for CPG Client's point-of-interest (POI) and location data. The assessment was conducted to identify data quality issues and provide actionable recommendations for improvement.

## Key Findings

Our analysis of 37,790 location records revealed several critical data quality issues:

- **Data Completeness Gaps**: 
  - 9.3% of records missing address data
  - 27.2% missing website information
  - Up to 76.7% missing operational hours (particularly on weekends)

- **Duplicate Records**: 
  - 132 potential duplicate business locations identified
  - Examples include "Silvercreek Realty Group" (5 duplicates) and "Americana Terrace" (4 duplicates)

- **Data Confidence Issues**: 
  - 37.3% of records have low confidence scores (below 0.7)
  - Average confidence score across all records: 0.745

- **Category Inconsistencies**: 
  - Potential misalignment between industry codes (NAICS) and business categories
  - 14 main categories, 30 sub-categories, but only 16 distinct full hierarchies

- **Format Standardization Issues**:
  - Multiple postal code formats (5-digit, 9-digit, and non-standard)
  - Character encoding problems in some business names

## Getting Started

### Prerequisites

To use the data quality tools in this repository:

```bash
pip install -r requirements.txt
```

### Running Data Quality Checks

The main data quality assessment script can be run with:

```bash
python data_quality_checker.py --table boisedemodatasampleaug
```

For automated monitoring, use:

```bash
python data_quality_monitor.py --schedule daily
```

## Repository Structure

```
.
├── README.md                   # This file
├── reports/                    # Detailed assessment reports
│   ├── executive_summary.md    # Executive summary for stakeholders
│   ├── technical_assessment.md # Detailed technical findings
│   └── improvement_plan.md     # Recommended improvement actions
├── src/                        # Source code for data quality tools
│   ├── data_quality_checker.py # Main assessment tools
│   ├── deduplication.py        # Deduplication utilities
│   └── validation_rules.py     # Data validation rules
├── sql/                        # SQL queries for data quality checks
│   ├── completeness.sql        # Completeness checks
│   ├── duplicates.sql          # Duplicate detection
│   └── consistency.sql         # Consistency validation
└── dashboard/                  # Data quality monitoring dashboard
    ├── app.py                  # Dashboard application
    └── metrics.py              # Data quality metrics definitions
```

## Improvement Recommendations

### Short-term Actions (0-3 months)

1. **Data Cleansing**:
   - Execute deduplication strategy with focus on high-priority duplicates
   - Standardize postal code formats
   - Fill critical data gaps (addresses, contact information)

2. **Validation Rules**:
   - Implement data entry validation for required fields
   - Establish consistent formatting rules

3. **Quick Wins**:
   - Correct city name inconsistencies
   - Resolve obvious category misclassifications

### Medium-term Initiatives (3-6 months)

1. **Process Improvements**:
   - Enhance data collection and validation workflows
   - Standardize category assignment processes

2. **Monitoring Implementation**:
   - Deploy data quality monitoring dashboard
   - Establish regular quality reporting

3. **Governance Framework**:
   - Define clear data ownership and stewardship
   - Document category taxonomy standards

### Long-term Strategy (6+ months)

1. **System Enhancements**:
   - Integrate automated data quality checks into ETL processes
   - Implement master data management practices

2. **Continuous Improvement**:
   - Establish data quality KPIs and targets
   - Implement regular data quality reviews


