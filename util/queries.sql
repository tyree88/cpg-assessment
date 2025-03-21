-- CPG CLIENT VALUABLE COLUMNS ANALYSIS
-- These queries highlight the most valuable data for CPG companies
-- focused on retail distribution, market analysis, and sales optimization


-- ===============================================
-- 1. STORE/RETAILER TARGETING & DISTRIBUTION PLANNING
-- ===============================================

-- 1.1 Active Retail & Grocery Distribution Points
-- Identifies open locations with complete address data for distribution planning
SELECT 
    dataplor_id,
    name,
    chain_name,
    main_category,
    sub_category,
    address,
    city,
    state,
    postal_code,
    latitude,
    longitude,
    open_closed_status,
    data_quality_confidence_score
FROM boisedemodatasampleaug
WHERE main_category IN ('retail', 'convenience_and_grocery_stores')
    AND open_closed_status = 'open'
    AND address IS NOT NULL
ORDER BY city, data_quality_confidence_score DESC;

-- 1.2 Distribution Window Analysis
-- Shows delivery window opportunities based on business hours
-- Critical for route planning and delivery scheduling
SELECT 
    dataplor_id,
    name,
    address,
    city,
    main_category,
    sub_category,
    CASE 
        WHEN monday_open IS NOT NULL THEN monday_open
        ELSE '09:00:00' -- Default assumption if missing
    END AS monday_open,
    CASE 
        WHEN monday_close IS NOT NULL THEN monday_close
        ELSE '17:00:00' -- Default assumption if missing
    END AS monday_close,
    -- Calculate estimated delivery window in hours
    CASE 
        WHEN monday_open IS NOT NULL AND monday_close IS NOT NULL 
        THEN EXTRACT(HOUR FROM monday_close) - EXTRACT(HOUR FROM monday_open)
        ELSE 8 -- Default assumption if missing
    END AS monday_window_hours
FROM boisedemodatasampleaug
WHERE main_category IN ('retail', 'convenience_and_grocery_stores')
    AND open_closed_status = 'open'
ORDER BY monday_window_hours DESC;

-- 1.3 Chain Store Priority Targeting
-- Identifies chains with multiple locations for efficient distribution deals
-- A single agreement with these chains can open multiple distribution points
SELECT 
    chain_name,
    COUNT(*) AS location_count,
    STRING_AGG(DISTINCT city, ', ') AS cities,
    MIN(data_quality_confidence_score) AS min_confidence,
    MAX(data_quality_confidence_score) AS max_confidence,
    AVG(data_quality_confidence_score) AS avg_confidence
FROM boisedemodatasampleaug
WHERE main_category IN ('retail', 'convenience_and_grocery_stores')
    AND open_closed_status = 'open'
    AND chain_id IS NOT NULL
    AND chain_id != ''
GROUP BY chain_name
HAVING COUNT(*) > 2
ORDER BY location_count DESC;

-- 1.4 Distribution Gaps Analysis
-- Identifies cities with limited retail coverage
-- Helps find underserved markets for targeted expansion
SELECT
    city,
    COUNT(*) AS total_locations,
    SUM(CASE WHEN main_category = 'retail' THEN 1 ELSE 0 END) AS retail_locations,
    SUM(CASE WHEN main_category = 'convenience_and_grocery_stores' THEN 1 ELSE 0 END) AS grocery_locations,
    (COUNT(*) * 1.0 / (SELECT COUNT(*) FROM boisedemodatasampleaug WHERE city = b.city)) * 100 AS retail_percentage
FROM boisedemodatasampleaug b
WHERE open_closed_status = 'open'
GROUP BY city
HAVING COUNT(*) > 20
ORDER BY retail_percentage;


-- ===============================================
-- 2. MARKET ANALYSIS & COMPETITIVE INTELLIGENCE
-- ===============================================

-- 2.1 Retail Segment Analysis
-- Identifies the distribution of retail categories
-- Critical for understanding market composition
SELECT 
    main_category,
    sub_category,
    COUNT(*) AS location_count,
    ROUND(AVG(CAST(popularity_score AS FLOAT))) AS avg_popularity,
    ROUND(AVG(CAST(sentiment_score AS FLOAT)), 2) AS avg_sentiment,
    COUNT(CASE WHEN price_level IS NOT NULL THEN 1 END) AS has_price_data,
    STRING_AGG(DISTINCT price_level, ', ') AS price_levels
FROM boisedemodatasampleaug
WHERE main_category IN ('retail', 'convenience_and_grocery_stores')
    AND open_closed_status = 'open'
GROUP BY main_category, sub_category
ORDER BY location_count DESC;

-- 2.2 Competitive Density Analysis
-- Maps retail density by postal code for competitive intelligence
-- Helps identify saturated vs. opportunity markets
SELECT 
    postal_code,
    city,
    COUNT(*) AS total_locations,
    SUM(CASE WHEN main_category = 'retail' THEN 1 ELSE 0 END) AS retail_locations,
    SUM(CASE WHEN main_category = 'convenience_and_grocery_stores' THEN 1 ELSE 0 END) AS grocery_locations,
    SUM(CASE WHEN chain_id IS NOT NULL AND chain_id != '' THEN 1 ELSE 0 END) AS chain_locations,
    STRING_AGG(DISTINCT sub_category, ', ') AS retail_types
FROM boisedemodatasampleaug
WHERE open_closed_status = 'open'
    AND postal_code IS NOT NULL
    AND main_category IN ('retail', 'convenience_and_grocery_stores')
GROUP BY postal_code, city
ORDER BY total_locations DESC;

-- 2.3 Popularity & Sentiment Comparison
-- Compares customer engagement metrics across retail categories 
-- Helps identify high-performing retail segments
SELECT 
    main_category,
    sub_category,
    COUNT(*) AS location_count,
    ROUND(AVG(CAST(popularity_score AS FLOAT)), 1) AS avg_popularity,
    ROUND(AVG(CAST(sentiment_score AS FLOAT)), 2) AS avg_sentiment,
    ROUND(AVG(CAST(dwell_time AS FLOAT)), 1) AS avg_dwell_minutes
FROM boisedemodatasampleaug
WHERE main_category IN ('retail', 'convenience_and_grocery_stores', 'dining')
    AND open_closed_status = 'open'
GROUP BY main_category, sub_category
HAVING COUNT(*) > 5
ORDER BY avg_popularity DESC, avg_sentiment DESC;

-- 2.4 Business Maturity Analysis
-- Analyzes retail location age for market maturity assessment
-- Helps understand market stability and turnover
SELECT
    main_category,
    sub_category,
    COUNT(*) AS location_count,
    AVG(EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM opened_on)) AS avg_years_open,
    COUNT(CASE WHEN opened_on > '2020-01-01' THEN 1 END) AS opened_since_2020,
    COUNT(CASE WHEN opened_on > '2020-01-01' THEN 1 END) * 100.0 / COUNT(*) AS pct_new_businesses
FROM boisedemodatasampleaug
WHERE main_category IN ('retail', 'convenience_and_grocery_stores')
    AND open_closed_status = 'open'
    AND opened_on IS NOT NULL
GROUP BY main_category, sub_category
HAVING COUNT(*) > 3
ORDER BY avg_years_open DESC;


-- ===============================================
-- 3. SALES TERRITORY MANAGEMENT
-- ===============================================

-- 3.1 Territory Coverage Analysis
-- Maps retail distribution by city for sales territory planning
-- Helps balance territories by opportunity size
SELECT 
    city,
    COUNT(*) AS total_locations,
    SUM(CASE WHEN main_category = 'retail' THEN 1 ELSE 0 END) AS retail_locations,
    SUM(CASE WHEN main_category = 'convenience_and_grocery_stores' THEN 1 ELSE 0 END) AS grocery_locations,
    SUM(CASE WHEN chain_id IS NOT NULL AND chain_id != '' THEN 1 ELSE 0 END) AS chain_locations,
    SUM(CASE WHEN chain_id IS NULL OR chain_id = '' THEN 1 ELSE 0 END) AS independent_locations,
    COUNT(DISTINCT sub_category) AS retail_category_diversity
FROM boisedemodatasampleaug
WHERE open_closed_status = 'open'
GROUP BY city
ORDER BY total_locations DESC;

-- 3.2 Account-Based Territory Mapping
-- Maps chain stores by territory for key account management
-- Helps organize territories around major chains
SELECT 
    chain_name,
    COUNT(*) AS total_locations,
    STRING_AGG(DISTINCT city, ', ') AS cities,
    SUM(CASE WHEN main_category = 'retail' THEN 1 ELSE 0 END) AS retail_locations,
    SUM(CASE WHEN main_category = 'convenience_and_grocery_stores' THEN 1 ELSE 0 END) AS grocery_locations
FROM boisedemodatasampleaug
WHERE chain_id IS NOT NULL
    AND chain_id != ''
    AND open_closed_status = 'open'
    AND main_category IN ('retail', 'convenience_and_grocery_stores')
GROUP BY chain_name
HAVING COUNT(*) > 1
ORDER BY total_locations DESC;

-- 3.3 Territory Workload Balancing
-- Calculates visit complexity by territory to balance workloads
-- Accounts for store types and operating hours for territory planning
SELECT 
    city AS territory,
    COUNT(*) AS total_accounts,
    SUM(CASE WHEN chain_id IS NOT NULL AND chain_id != '' THEN 1 ELSE 0 END) AS chain_accounts,
    SUM(CASE WHEN chain_id IS NULL OR chain_id = '' THEN 1 ELSE 0 END) AS independent_accounts,
    -- Calculate workload indicators
    SUM(CASE WHEN monday_open IS NOT NULL AND monday_close IS NOT NULL THEN 1 ELSE 0 END) AS accounts_with_hours,
    AVG(CASE 
        WHEN monday_open IS NOT NULL AND monday_close IS NOT NULL 
        THEN EXTRACT(HOUR FROM monday_close) - EXTRACT(HOUR FROM monday_open)
        ELSE 8 END) AS avg_business_hours,
    COUNT(DISTINCT sub_category) AS account_type_diversity
FROM boisedemodatasampleaug
WHERE main_category IN ('retail', 'convenience_and_grocery_stores')
    AND open_closed_status = 'open'
GROUP BY city
ORDER BY total_accounts DESC;

-- 3.4 Geographic Cluster Analysis
-- Identifies retail clusters for efficient territory visits
-- Groups nearby locations for optimized sales routes
SELECT 
    postal_code,
    ROUND(latitude, 2) AS latitude_area,
    ROUND(longitude, 2) AS longitude_area,
    COUNT(*) AS locations_in_cluster,
    STRING_AGG(DISTINCT main_category, ', ') AS business_types,
    STRING_AGG(DISTINCT chain_name, ', ') AS chains_in_area
FROM boisedemodatasampleaug
WHERE main_category IN ('retail', 'convenience_and_grocery_stores')
    AND open_closed_status = 'open'
    AND latitude IS NOT NULL
    AND longitude IS NOT NULL
GROUP BY postal_code, ROUND(latitude, 2), ROUND(longitude, 2)
HAVING COUNT(*) > 3
ORDER BY locations_in_cluster DESC;


-- ===============================================
-- 4. CONSUMER TARGETING & MARKETING CAMPAIGNS
-- ===============================================

-- 4.1 Digital Presence Analysis
-- Evaluates retail digital presence for omnichannel marketing
-- Identifies gaps in online presence for digital campaign planning
SELECT 
    main_category,
    sub_category,
    COUNT(*) AS total_locations,
    SUM(CASE WHEN website IS NOT NULL THEN 1 ELSE 0 END) AS has_website,
    SUM(CASE WHEN website IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS website_coverage_pct,
    AVG(CAST(popularity_score AS FLOAT)) AS avg_popularity,
    AVG(CAST(sentiment_score AS FLOAT)) AS avg_sentiment
FROM boisedemodatasampleaug
WHERE main_category IN ('retail', 'convenience_and_grocery_stores')
    AND open_closed_status = 'open'
GROUP BY main_category, sub_category
HAVING COUNT(*) > 5
ORDER BY website_coverage_pct, total_locations DESC;

-- 4.2 Consumer Sentiment Analysis
-- Analyzes sentiment data for targeted marketing
-- Identifies high-sentiment locations for premium product placement
SELECT 
    main_category,
    sub_category,
    COUNT(*) AS total_locations,
    ROUND(AVG(CAST(sentiment_score AS FLOAT)), 2) AS avg_sentiment,
    COUNT(CASE WHEN CAST(sentiment_score AS FLOAT) >= 4.0 THEN 1 END) AS high_sentiment_locations,
    COUNT(CASE WHEN CAST(sentiment_score AS FLOAT) >= 4.0 THEN 1 END) * 100.0 / COUNT(*) AS high_sentiment_pct,
    ROUND(AVG(CAST(popularity_score AS FLOAT)), 2) AS avg_popularity
FROM boisedemodatasampleaug
WHERE main_category IN ('retail', 'convenience_and_grocery_stores')
    AND open_closed_status = 'open'
    AND sentiment_score IS NOT NULL
GROUP BY main_category, sub_category
HAVING COUNT(*) > 5
ORDER BY avg_sentiment DESC, high_sentiment_pct DESC;

-- 4.3 Premium Retail Location Targeting
-- Identifies premium retail locations for high-end product placement
-- Combines multiple quality indicators for targeted marketing
SELECT 
    dataplor_id,
    name,
    main_category,
    sub_category,
    address,
    city,
    chain_name,
    price_level,
    CAST(popularity_score AS FLOAT) AS popularity,
    CAST(sentiment_score AS FLOAT) AS sentiment,
    CAST(dwell_time AS FLOAT) AS dwell_minutes,
    website
FROM boisedemodatasampleaug
WHERE main_category IN ('retail', 'convenience_and_grocery_stores')
    AND open_closed_status = 'open'
    AND (price_level = 'expensive' OR price_level = 'very expensive' 
         OR CAST(sentiment_score AS FLOAT) >= 4.5
         OR CAST(popularity_score AS FLOAT) >= 4)
    AND data_quality_confidence_score > 0.8
ORDER BY CAST(sentiment_score AS FLOAT) DESC, CAST(popularity_score AS FLOAT) DESC;

-- 4.4 Shopping Behavior Analysis
-- Analyzes dwell time to understand shopping duration
-- Critical for in-store marketing planning and product placement
SELECT 
    main_category,
    sub_category,
    COUNT(*) AS location_count,
    ROUND(AVG(CAST(dwell_time AS FLOAT)), 1) AS avg_dwell_minutes,
    MIN(CAST(dwell_time AS FLOAT)) AS min_dwell_minutes,
    MAX(CAST(dwell_time AS FLOAT)) AS max_dwell_minutes,
    NTILE(4) OVER (ORDER BY AVG(CAST(dwell_time AS FLOAT))) AS dwell_quartile
FROM boisedemodatasampleaug
WHERE main_category IN ('retail', 'convenience_and_grocery_stores')
    AND open_closed_status = 'open'
    AND dwell_time IS NOT NULL
GROUP BY main_category, sub_category
HAVING COUNT(*) > 3
ORDER BY avg_dwell_minutes DESC;


-- ===============================================
-- 5. SUPPLY CHAIN OPTIMIZATION
-- ===============================================

-- 5.1 Delivery Window Optimization
-- Analyzes business hours for optimal delivery scheduling
-- Identifies patterns for efficient supply chain planning
SELECT 
    EXTRACT(HOUR FROM monday_open) AS opening_hour,
    COUNT(*) AS location_count,
    STRING_AGG(DISTINCT main_category, ', ') AS business_types,
    STRING_AGG(DISTINCT sub_category, ', ') AS sub_categories,
    STRING_AGG(DISTINCT city, ', ') AS cities
FROM boisedemodatasampleaug
WHERE main_category IN ('retail', 'convenience_and_grocery_stores')
    AND open_closed_status = 'open'
    AND monday_open IS NOT NULL
GROUP BY EXTRACT(HOUR FROM monday_open)
ORDER BY opening_hour;

-- 5.2 Chain Distribution Efficiency
-- Analyzes chain store geographic distribution for warehouse planning
-- Helps optimize inventory distribution across chain locations
SELECT 
    chain_name,
    COUNT(*) AS total_locations,
    COUNT(DISTINCT city) AS cities_covered,
    COUNT(DISTINCT postal_code) AS zip_codes_covered,
    MAX(latitude) - MIN(latitude) AS lat_spread,
    MAX(longitude) - MIN(longitude) AS long_spread,
    STRING_AGG(DISTINCT city, ', ') AS cities
FROM boisedemodatasampleaug
WHERE main_category IN ('retail', 'convenience_and_grocery_stores')
    AND open_closed_status = 'open'
    AND chain_id IS NOT NULL
    AND chain_id != ''
    AND latitude IS NOT NULL
    AND longitude IS NOT NULL
GROUP BY chain_name
HAVING COUNT(*) > 2
ORDER BY total_locations DESC, cities_covered DESC;

-- 5.3 Weekend vs. Weekday Operations
-- Compares weekend vs. weekday hours for distribution planning
-- Critical for scheduling deliveries efficiently
SELECT 
    main_category,
    sub_category,
    COUNT(*) AS total_locations,
    SUM(CASE WHEN monday_open IS NOT NULL AND monday_close IS NOT NULL THEN 1 ELSE 0 END) AS monday_open_count,
    SUM(CASE WHEN saturday_open IS NOT NULL AND saturday_close IS NOT NULL THEN 1 ELSE 0 END) AS saturday_open_count,
    SUM(CASE WHEN sunday_open IS NOT NULL AND sunday_close IS NOT NULL THEN 1 ELSE 0 END) AS sunday_open_count,
    SUM(CASE WHEN sunday_open IS NOT NULL AND sunday_close IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / 
        SUM(CASE WHEN monday_open IS NOT NULL AND monday_close IS NOT NULL THEN 1 ELSE 0 END) AS sunday_vs_monday_pct
FROM boisedemodatasampleaug
WHERE main_category IN ('retail', 'convenience_and_grocery_stores')
    AND open_closed_status = 'open'
GROUP BY main_category, sub_category
HAVING COUNT(*) > 5
ORDER BY sunday_vs_monday_pct DESC;

-- 5.4 Geo-Cluster Route Optimization
-- Groups locations by proximity for route optimization
-- Helps design efficient delivery routes
WITH location_clusters AS (
    SELECT
        ROUND(latitude, 2) AS lat_cluster,
        ROUND(longitude, 2) AS lng_cluster,
        COUNT(*) AS locations_in_cluster
    FROM boisedemodatasampleaug
    WHERE main_category IN ('retail', 'convenience_and_grocery_stores')
        AND open_closed_status = 'open'
        AND latitude IS NOT NULL
        AND longitude IS NOT NULL
    GROUP BY ROUND(latitude, 2), ROUND(longitude, 2)
    HAVING COUNT(*) > 1
)
SELECT
    a.dataplor_id,
    a.name,
    a.address,
    a.city,
    a.main_category,
    a.sub_category,
    ROUND(a.latitude, 2) AS lat_cluster,
    ROUND(a.longitude, 2) AS lng_cluster,
    b.locations_in_cluster,
    a.monday_open,
    a.monday_close
FROM boisedemodatasampleaug a
JOIN location_clusters b ON ROUND(a.latitude, 2) = b.lat_cluster AND ROUND(a.longitude, 2) = b.lng_cluster
WHERE a.main_category IN ('retail', 'convenience_and_grocery_stores')
    AND a.open_closed_status = 'open'
ORDER BY b.locations_in_cluster DESC, lat_cluster, lng_cluster;


-- ===============================================
-- 6. DATA QUALITY ASSESSMENT
-- ===============================================

-- 6.1 Critical Data Completeness for CPG Distribution
-- Assesses completeness of critical fields for CPG operations
-- Helps prioritize data quality improvement efforts
SELECT
    'Distribution Data Quality' AS assessment,
    COUNT(*) AS total_locations,
    SUM(CASE WHEN address IS NULL THEN 1 ELSE 0 END) AS missing_address,
    SUM(CASE WHEN address IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_address_pct,
    SUM(CASE WHEN monday_open IS NULL OR monday_close IS NULL THEN 1 ELSE 0 END) AS missing_hours,
    SUM(CASE WHEN monday_open IS NULL OR monday_close IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_hours_pct,
    SUM(CASE WHEN website IS NULL THEN 1 ELSE 0 END) AS missing_website,
    SUM(CASE WHEN website IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS missing_website_pct,
    SUM(CASE WHEN data_quality_confidence_score < 0.7 THEN 1 ELSE 0 END) AS low_confidence_records,
    SUM(CASE WHEN data_quality_confidence_score < 0.7 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS low_confidence_pct
FROM boisedemodatasampleaug
WHERE main_category IN ('retail', 'convenience_and_grocery_stores')
    AND open_closed_status = 'open';

-- 6.2 Specific Chain Data Quality
-- Assesses data quality for important CPG retail chains
-- Helps prioritize data enrichment for key accounts
SELECT 
    chain_name,
    COUNT(*) AS total_locations,
    SUM(CASE WHEN address IS NULL THEN 1 ELSE 0 END) AS missing_address,
    SUM(CASE WHEN monday_open IS NULL OR monday_close IS NULL THEN 1 ELSE 0 END) AS missing_hours,
    SUM(CASE WHEN website IS NULL THEN 1 ELSE 0 END) AS missing_website,
    ROUND(AVG(data_quality_confidence_score), 2) AS avg_confidence_score
FROM boisedemodatasampleaug
WHERE main_category IN ('retail', 'convenience_and_grocery_stores')
    AND open_closed_status = 'open'
    AND chain_id IS NOT NULL
    AND chain_id != ''
GROUP BY chain_name
HAVING COUNT(*) > 3
ORDER BY total_locations DESC, avg_confidence_score;