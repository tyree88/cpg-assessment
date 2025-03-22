import streamlit as st

def render_overview():
    """Render the main overview page with key findings and getting started information."""
    st.markdown('<h2 class="sub-header">CPG Data Quality Assessment Overview</h2>', unsafe_allow_html=True)
    
    # Executive Summary Section
    st.markdown("### 📊 Executive Summary")
    st.markdown("""
    This report presents a comprehensive assessment of data quality for the point-of-interest (POI) and location data 
    stored in the client's database. The assessment reveals several critical data quality issues that require attention 
    to improve business decision-making and operational efficiency.

    The client's database contains **37,790 records** of business location data primarily in Idaho, with comprehensive 
    information covering business identifiers, categorization, location information, contact details, operational data, 
    and quality metrics.
    """)
    
    # Key Findings Section using columns
    st.markdown("### 🔍 Key Data Quality Issues")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Data Completeness Issues
        with st.container():
            st.markdown('<p class="critical-issue"><strong>Data Completeness Issues</strong></p>', unsafe_allow_html=True)
            st.markdown("""
            - 9.3% missing address data
            - 27.2% missing website information
            - 38.3% missing Monday business hours
            - Up to 76.7% missing Sunday business hours
            """)
        
        # Duplicate Records
        with st.container():
            st.markdown('<p class="warning-issue"><strong>Duplicate Records</strong></p>', unsafe_allow_html=True)
            st.markdown("""
            - 132 potential duplicate locations identified
            - Notable examples:
              - "Silvercreek Realty Group" (5 instances)
              - "Americana Terrace" (4 instances)
            """)
        
        # Data Confidence
        with st.container():
            st.markdown('<p class="warning-issue"><strong>Data Confidence Concerns</strong></p>', unsafe_allow_html=True)
            st.markdown("""
            - 37.3% of records have low confidence scores (below 0.7)
            - Only 5.7% have high confidence scores (above 0.9)
            - Average confidence score: 0.745
            """)
    
    with col2:
        # Category Issues
        with st.container():
            st.markdown('<p class="critical-issue"><strong>Category Inconsistencies</strong></p>', unsafe_allow_html=True)
            st.markdown("""
            - Potential misalignments in category hierarchies
            - Subcategories used incorrectly across main categories
            - Limited distinct full hierarchies despite many categories
            """)
        
        # Format Issues
        with st.container():
            st.markdown('<p class="warning-issue"><strong>Format Standardization Issues</strong></p>', unsafe_allow_html=True)
            st.markdown("""
            Postal code format inconsistencies:
            - 36,949 records: 5-digit format
            - 521 records: 9-digit format
            - 79 records: non-standard lengths
            """)
    
    # Business Impact Section
    st.markdown("### 💼 Business Impact")
    st.markdown("""
    Missing operational data (hours, websites) directly impacts customer experience and engagement. 
    Incomplete address information limits the utility of location-based analytics. Additionally:
    - Duplicates inflate location counts
    - Skewed market analysis
    - Customer confusion through inconsistent information
    """)
    
    # Business Operations Impact
    st.markdown("#### Critical Impact on CPG Operations")
    impact_col1, impact_col2 = st.columns(2)
    
    with impact_col1:
        st.markdown("""
        These issues directly affect the ability to:
        - Target retail distribution points
        - Plan delivery routes and schedules
        - Analyze competitive market landscapes
        """)
    
    with impact_col2:
        st.markdown("""
        Additional operational impacts:
        - Manage sales territories
        - Design consumer marketing campaigns
        - Optimize supply chain operations
        """)
    
    # Scalable Data Management Section
    st.markdown("### 🔄 Scalable Data Quality Management Framework")
    st.markdown("""
    The proposed framework for scalable data quality management features a three-layer architecture: Ingestion, Cleaning, and Distribution.

    It uses Airbyte for automated data collection, initially storing data in DuckDB before transitioning to ClickHouse for larger-scale analytics. Continuous data quality monitoring is handled by Soda, while Prefect manages the workflow with event orchestration and alerts. For data transformation, dbt is utilized, connected to Databricks for advanced analytics.

    The system operates on a hybrid cloud architecture using Google Kubernetes Engine, supporting scalability and consistent data quality, with capabilities for automated validation, real-time monitoring, and machine learning
    """)
    
    scale_col1, scale_col2, scale_col3 = st.columns(3)
    
    with scale_col1:
        st.markdown("""
        #### 🔍 Automated Validation Pipeline
        
        **Data Ingestion & Validation:**
        - Input validation at collection points using Airbye
        - Initial storage in DuckDB (POC phase)
        - Migration to ClickHouse for scaled analytics
        - Soda integration for quality monitoring
        
        **Pipeline Orchestration:**
        - Prefect for event orchestration
        - Real-time quality checks
        - Automated alerting system
        - Clear visibility dashboards
        """)
    
    with scale_col2:
        st.markdown("""
        #### 📊 Model Analysis & Development
        
        **Data Transformation:**
        - dbt for model development
        - Version-controlled transformations
        - Automated testing suite
        
        **Advanced Analytics:**
        - Databricks integration
        - Scalable computation
        - Machine learning capabilities
        - Automated model retraining
        """)
    
    with scale_col3:
        st.markdown("""
        #### 🚀 Scalable Architecture
        
        **Infrastructure:**
        - Hybrid cloud distributed system
        - GKE for model deployment
        - Auto-scaling capabilities
        
        **Data Flow:**
        - Streaming data processing
        - Batch processing for historical data
        - Real-time quality monitoring
        - Automated recovery procedures
        """)
    
    # Architecture Diagram
    st.markdown("#### System Architecture Overview")
    
    # Add Miro board link
    st.markdown("""
    > 🔗 [View interactive architecture diagram in Miro](https://miro.com/app/board/uXjVIMv-I3g=/?share_link_id=895951754429)
    """)
    
    # Create columns for better layout
    img_col1, img_col2 = st.columns([2, 1])
    
    with img_col1:
        # Add the detailed architecture image
        st.image("assets/architecture_diagram.jpg", 
                caption="Data Quality Pipeline Architecture",
                use_container_width=True)
    
    with img_col2:
        st.markdown("""
        #### Key Components:
        
        🔄 **Data Flow**
        - Data ingestion via Airbyte
        - Quality validation with Soda
        - Transformation using dbt
        
        🛠️ **Tools**
        - DuckDB/ClickHouse for storage
        - Prefect for orchestration
        - Databricks for analytics
        
        ☁️ **Infrastructure**
        - GKE for deployment
        - Hybrid cloud setup
        - Auto-scaling enabled
        """)
    # Implementation Notes
    with st.expander("📝 Implementation Notes"):
        st.markdown("""
        **Phase 1: Foundation**
        - Set up DuckDB for initial data storage
        - Implement basic Airbye validation
        - Configure Prefect workflows
        
        **Phase 2: Scaling**
        - Migrate to ClickHouse for larger datasets
        - Integrate Soda for comprehensive monitoring
        - Implement dbt models
        
        **Phase 3: Enterprise**
        - Deploy on GKE
        - Integrate with Databricks
        - Implement full automation
        
        > **Note:** This architecture ensures scalability from thousands to millions of records while maintaining data quality standards.
        """)
    
    # Recommendations Section
    st.markdown("### 📋 Next Steps")
    st.markdown("""
    1. Review the **Data Analysis** tab for detailed insights into each issue
    2. Use the **Data Cleaning** tab to address identified problems
    3. Explore **CPG Queries** tab for industry-specific analysis
    4. Generate a comprehensive report in the **Report** tab
    """)
    
    # Data Privacy Note
    st.markdown("""
    ---
    > **Note:** All analysis is performed locally on your data. No information is sent to external servers.
    """) 