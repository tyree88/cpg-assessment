#!/usr/bin/env python3
"""
CPG Analysis Example Script

This script demonstrates how to use the cpg_analysis module to analyze
CPG (Consumer Packaged Goods) data in a DuckDB database.
"""

from util import cpg_analysis
import matplotlib.pyplot as plt
import seaborn as sns
import os

# Create output directory for visualizations
os.makedirs('output', exist_ok=True)

# Connect to the database
try:
    # Update this path to your actual DuckDB database location
    conn, table_name = cpg_analysis.connect_db('my_db')
    print(f"Connected to database, using table: {table_name}")
    
    # Example 1: Chain Store Analysis
    print("\n--- Chain Store Analysis ---")
    chains = cpg_analysis.identify_chain_store_targets(conn, min_locations=3)
    print(f"Found {len(chains)} chains with 3+ locations")
    print(chains.head())
    
    # Visualize top chains
    if not chains.empty:
        plt.figure(figsize=(12, 6))
        top_chains = chains.head(10)
        sns.barplot(x='location_count', y='chain_name', data=top_chains)
        plt.title('Top Retail Chains for CPG Distribution')
        plt.xlabel('Number of Locations')
        plt.ylabel('Chain Name')
        plt.tight_layout()
        plt.savefig('output/top_chains.png')
        print("Saved chain visualization to output/top_chains.png")
    
    # Example 2: Data Completeness Analysis
    print("\n--- Data Completeness Analysis ---")
    completeness = cpg_analysis.assess_critical_data_completeness(conn)
    
    # Print data quality issues
    if not completeness.empty:
        missing_address_pct = float(completeness['missing_address_pct'].iloc[0])
        missing_hours_pct = float(completeness['missing_hours_pct'].iloc[0])
        missing_website_pct = float(completeness['missing_website_pct'].iloc[0])
        low_confidence_pct = float(completeness['low_confidence_pct'].iloc[0])
        
        print("Data Quality Issues:")
        print(f"- Missing Addresses: {missing_address_pct:.1f}%")
        print(f"- Missing Hours: {missing_hours_pct:.1f}%")
        print(f"- Missing Websites: {missing_website_pct:.1f}%")
        print(f"- Low Confidence Records: {low_confidence_pct:.1f}%")
        
        # Visualize data completeness
        plt.figure(figsize=(10, 6))
        categories = ['Addresses', 'Hours', 'Websites', 'High Confidence']
        complete_values = [100 - missing_address_pct, 100 - missing_hours_pct, 
                          100 - missing_website_pct, 100 - low_confidence_pct]
        missing_values = [missing_address_pct, missing_hours_pct, 
                         missing_website_pct, low_confidence_pct]
        
        bar_width = 0.35
        index = range(len(categories))
        
        plt.bar(index, complete_values, bar_width, label='Complete', color='green')
        plt.bar(index, missing_values, bar_width, bottom=complete_values, label='Missing', color='red')
        
        plt.xlabel('Data Category')
        plt.ylabel('Percentage')
        plt.title('Data Completeness Assessment')
        plt.xticks(index, categories)
        plt.ylim(0, 100)
        plt.legend()
        plt.tight_layout()
        plt.savefig('output/data_completeness.png')
        print("Saved data completeness visualization to output/data_completeness.png")
    
    # Example 3: Territory Coverage Analysis
    print("\n--- Territory Coverage Analysis ---")
    territories = cpg_analysis.analyze_territory_coverage(conn)
    print(f"Analyzed {len(territories)} territories")
    print(territories.head(5))
    
    # Visualize territory coverage
    if not territories.empty:
        plt.figure(figsize=(12, 6))
        top_territories = territories.head(10)
        
        # Create a stacked bar chart
        plt.bar(top_territories['city'], top_territories['retail_locations'], 
                label='Retail', color='skyblue')
        plt.bar(top_territories['city'], top_territories['grocery_locations'], 
                bottom=top_territories['retail_locations'], label='Grocery', color='orange')
        
        plt.title('Retail and Grocery Distribution by City')
        plt.xlabel('City')
        plt.ylabel('Number of Locations')
        plt.xticks(rotation=45, ha='right')
        plt.legend()
        plt.tight_layout()
        plt.savefig('output/territory_coverage.png')
        print("Saved territory coverage visualization to output/territory_coverage.png")
    
    # Example 4: Run a comprehensive analysis and save all results to CSV
    print("\n--- Running Comprehensive Analysis ---")
    all_results = cpg_analysis.run_all_analyses(conn)
    
    # Save all results to CSV files
    for name, df in all_results.items():
        output_path = f"output/{name}.csv"
        df.to_csv(output_path, index=False)
    
    print("Saved all analysis results to output/ directory")
    print(f"Generated {len(all_results)} analysis files")

except Exception as e:
    print(f"Error: {str(e)}")

print("\nAnalysis complete!")
