#!/usr/bin/env python3
"""
Data Quality Module

Provides functions for assessing and reporting on data quality with a focus on sample datasets.
Implements a simplified approach to identify common data quality issues.
"""

import pandas as pd
from typing import Dict, Any, List, Optional
from enum import Enum, auto
from prefect import task
from pydantic import BaseModel, Field, field_validator
from datetime import datetime

# Simple logging with print statements


class IssueLevel(Enum):
    """Enum representing the severity level of a data quality issue."""
    CRITICAL = auto()
    WARNING = auto()
    INFO = auto()
    
    def to_string(self) -> str:
        """Convert enum to string representation."""
        return self.name.lower()


class Issue(BaseModel):
    """Model representing a data quality issue."""
    level: str
    type: str
    description: str
    affected_columns: Optional[List[str]] = None
    details: Optional[Dict[str, Any]] = None
    
    @field_validator('level')
    @classmethod
    def validate_level(cls, v: str) -> str:
        """Validate that level is one of the allowed values."""
        allowed_levels = [level.to_string() for level in IssueLevel]
        if v not in allowed_levels:
            raise ValueError(f"Level must be one of {allowed_levels}")
        return v


class QualityDeduction(BaseModel):
    """Model for quality score deductions."""
    reason: str
    deduction: float
    details: str


class QualityScore(BaseModel):
    """Model for quality score calculation result."""
    score: float
    deductions: List[QualityDeduction]


class QualityReport(BaseModel):
    """Model for quality report."""
    table_name: str
    quality_score: float
    issues_summary: Dict[str, int]
    column_quality: Dict[str, float]
    recommendations: List[str]
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())


@task
def identify_data_quality_issues(df: pd.DataFrame, analysis: Dict[str, Any], table_name: str) -> Dict[str, Any]:
    """
    Identify data quality issues with a focus on sample datasets.
    
    Args:
        df: Pandas DataFrame containing the data
        analysis: Dictionary containing analysis results
        table_name: Name of the table being analyzed
        
    Returns:
        Dictionary containing identified issues
    """
    issues = {
        "critical": [],
        "warning": [],
        "info": []
    }
    
    # Check for missing values
    if 'missing_values' in analysis:
        for col, info in analysis['missing_values'].items():
            if info['percent'] > 20.0:  # Critical threshold
                issues["critical"].append(Issue(
                    level=IssueLevel.CRITICAL.to_string(),
                    type='high_missing_values',
                    description=f"High percentage of missing values in column {col}: {info['percent']:.2f}%",
                    affected_columns=[col],
                    details=info
                ))
            elif info['percent'] > 5.0:  # Warning threshold
                issues["warning"].append(Issue(
                    level=IssueLevel.WARNING.to_string(),
                    type='medium_missing_values',
                    description=f"Medium percentage of missing values in column {col}: {info['percent']:.2f}%",
                    affected_columns=[col],
                    details=info
                ))
            elif info['percent'] > 0.0:  # Info threshold
                issues["info"].append(Issue(
                    level=IssueLevel.INFO.to_string(),
                    type='low_missing_values',
                    description=f"Low percentage of missing values in column {col}: {info['percent']:.2f}%",
                    affected_columns=[col],
                    details=info
                ))
    
    # Check for duplicate rows
    if 'duplicate_rows' in analysis and 'percent' in analysis['duplicate_rows']:
        dup_percent = analysis['duplicate_rows']['percent']
        if dup_percent > 10.0:  # Critical threshold
            issues["critical"].append(Issue(
                level=IssueLevel.CRITICAL.to_string(),
                type='high_duplicate_rows',
                description=f"High percentage of duplicate rows: {dup_percent:.2f}%",
                details=analysis['duplicate_rows']
            ))
        elif dup_percent > 1.0:  # Warning threshold
            issues["warning"].append(Issue(
                level=IssueLevel.WARNING.to_string(),
                type='medium_duplicate_rows',
                description=f"Medium percentage of duplicate rows: {dup_percent:.2f}%",
                details=analysis['duplicate_rows']
            ))
        elif dup_percent > 0.0:  # Info threshold
            issues["info"].append(Issue(
                level=IssueLevel.INFO.to_string(),
                type='low_duplicate_rows',
                description=f"Low percentage of duplicate rows: {dup_percent:.2f}%",
                details=analysis['duplicate_rows']
            ))
    
    # Check for data type inconsistencies
    if 'data_types' in analysis:
        mixed_type_cols = [col for col, types in analysis['data_types'].items() 
                          if isinstance(types, list) and len(types) > 1]
        if mixed_type_cols:
            issues["warning"].append(Issue(
                level=IssueLevel.WARNING.to_string(),
                type='mixed_data_types',
                description=f"Mixed data types detected in {len(mixed_type_cols)} columns",
                affected_columns=mixed_type_cols,
                details={col: analysis['data_types'][col] for col in mixed_type_cols}
            ))
    
    # Calculate quality score
    quality_score = 100.0
    deductions = []
    
    # Deduct for critical issues
    for issue in issues["critical"]:
        deduction = 10.0
        quality_score -= deduction
        deductions.append(QualityDeduction(
            reason=issue.type,
            deduction=deduction,
            details=issue.description
        ))
    
    # Deduct for warning issues
    for issue in issues["warning"]:
        deduction = 5.0
        quality_score -= deduction
        deductions.append(QualityDeduction(
            reason=issue.type,
            deduction=deduction,
            details=issue.description
        ))
    
    # Deduct for info issues
    for issue in issues["info"]:
        deduction = 1.0
        quality_score -= deduction
        deductions.append(QualityDeduction(
            reason=issue.type,
            deduction=deduction,
            details=issue.description
        ))
    
    # Ensure quality score is between 0 and 100
    quality_score = max(0.0, min(100.0, quality_score))
    
    return {
        "table_name": table_name,
        "issues": issues,
        "quality_score": QualityScore(
            score=quality_score,
            deductions=deductions
        )
    }


@task
def generate_quality_report(analysis: Dict[str, Any], quality_result: Dict[str, Any]) -> QualityReport:
    """
    Generate a comprehensive data quality report
    
    Args:
        analysis: Dictionary containing analysis results
        quality_result: Dictionary containing quality assessment results from identify_data_quality_issues
        
    Returns:
        QualityReport with the quality report data
    """
    # Count issues by level
    issues_summary = {
        "critical": len(quality_result["issues"]["critical"]),
        "warning": len(quality_result["issues"]["warning"]),
        "info": len(quality_result["issues"]["info"])
    }
    
    # Calculate column quality scores
    column_quality = {}
    if 'missing_values' in analysis:
        for col, info in analysis['missing_values'].items():
            # Simple quality score based on percentage of missing values
            # 100% - missing_percent
            column_quality[col] = 100.0 - info['percent']
    
    # Get recommendations from the quality issues
    recommendations = recommend_cleaning_operations(analysis, quality_result["issues"])
    
    return QualityReport(
        table_name=quality_result["table_name"],
        quality_score=quality_result["quality_score"].score,
        issues_summary=issues_summary,
        column_quality=column_quality,
        recommendations=recommendations
    )


def recommend_cleaning_operations(analysis: Dict[str, Any], issues: Dict[str, List[Issue]]) -> List[str]:
    """
    Recommend data cleaning operations based on quality issues
    
    Args:
        analysis: Dictionary containing analysis results
        issues: Dictionary containing identified issues by level
        
    Returns:
        List of recommended cleaning operations
    """
    recommendations = []
    
    # Handle missing values
    missing_value_issues = [issue for level in issues.values() 
                           for issue in level if 'missing_values' in issue.type]
    if missing_value_issues:
        critical_missing = [issue for issue in issues["critical"] if 'missing_values' in issue.type]
        if critical_missing:
            affected_cols = [col for issue in critical_missing for col in (issue.affected_columns or [])]
            if affected_cols:
                recommendations.append(f"Consider dropping columns with high missing values: {', '.join(affected_cols)}")
        
        warning_missing = [issue for issue in issues["warning"] if 'missing_values' in issue.type]
        if warning_missing:
            affected_cols = [col for issue in warning_missing for col in (issue.affected_columns or [])]
            if affected_cols:
                recommendations.append(f"Impute missing values in columns: {', '.join(affected_cols)}")
    
    # Handle duplicate rows
    duplicate_issues = [issue for level in issues.values() 
                       for issue in level if 'duplicate_rows' in issue.type]
    if duplicate_issues:
        critical_dupes = [issue for issue in issues["critical"] if 'duplicate_rows' in issue.type]
        if critical_dupes:
            recommendations.append("Remove duplicate rows from the dataset")
    
    # Handle mixed data types
    mixed_type_issues = [issue for level in issues.values() 
                        for issue in level if 'mixed_data_types' in issue.type]
    if mixed_type_issues:
        for issue in mixed_type_issues:
            affected_cols = issue.affected_columns or []
            if affected_cols:
                recommendations.append(f"Standardize data types in columns: {', '.join(affected_cols)}")
    
    # Add general recommendations if there are issues
    if any(len(issues[level]) > 0 for level in issues):
        recommendations.append("Run a data profiling tool for more detailed analysis")
        recommendations.append("Document data quality issues and cleaning steps for reproducibility")
    
    return recommendations