-- ====================================================================================
-- M-to-SQL Transformation Script
-- ====================================================================================
-- Dataset: COVID Bakeoff PBIR
-- Workspace: Auto_DP
-- Dataset ID: 4b08eee3-3c01-441c-b7bd-0ec50e7aa12f
-- Table: Global_Mobility_Report
-- Expression Type: table
-- Object Name: N/A
-- Target Table: stg_Global_Mobility_Report
-- Generated: 2025-11-04 10:49:28
-- ====================================================================================
-- Original M Code Reference:
-- let
    Source = Csv.Document(Web.Contents("https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv"),[Delimiter=",", Columns=15, Encoding=1252, QuoteStyle=QuoteStyle.None]),
    #"Promote...
-- ====================================================================================

```sql
-- =============================================
-- T-SQL Transformation of Global_Mobility_Report
-- =============================================
-- PREREQUISITE: Data must be loaded from CSV source into stg_Global_Mobility_Report
-- Source: https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv
-- =============================================

SELECT
    -- Retained columns with type casting (M: Table.TransformColumnTypes)
    CAST(country_region_code AS VARCHAR(MAX)) AS country_region_code,
    CAST(country_region AS VARCHAR(MAX)) AS country_region,
    CAST(sub_region_1 AS VARCHAR(MAX)) AS sub_region_1,
    CAST(sub_region_2 AS VARCHAR(MAX)) AS sub_region_2,
    CAST(metro_area AS VARCHAR(MAX)) AS metro_area,
    -- iso_3166_2_code excluded (M: Table.RemoveColumns)
    -- census_fips_code excluded (M: Table.RemoveColumns)
    -- place_id excluded (M: Table.RemoveColumns)
    CAST([date] AS DATE) AS [date],
    CAST(retail_and_recreation_percent_change_from_baseline AS BIGINT) AS retail_and_recreation_percent_change_from_baseline,
    CAST(grocery_and_pharmacy_percent_change_from_baseline AS BIGINT) AS grocery_and_pharmacy_percent_change_from_baseline,
    CAST(parks_percent_change_from_baseline AS BIGINT) AS parks_percent_change_from_baseline,
    CAST(transit_stations_percent_change_from_baseline AS BIGINT) AS transit_stations_percent_change_from_baseline,
    CAST(workplaces_percent_change_from_baseline AS BIGINT) AS workplaces_percent_change_from_baseline,
    CAST(residential_percent_change_from_baseline AS BIGINT) AS residential_percent_change_from_baseline
FROM 
    stg_Global_Mobility_Report
WHERE 
    -- M: Table.SelectRows filter condition (each ([sub_region_1] = ""))
    sub_region_1 = ''
    OR sub_region_1 IS NULL; -- Handle potential NULL values for empty string equivalence

-- =============================================
-- TRANSFORMATION NOTES:
-- =============================================
-- 1. Removed Columns (M: Table.RemoveColumns):
--    - census_fips_code
--    - place_id
--    - iso_3166_2_code
--
-- 2. Filtered Rows (M: Table.SelectRows):
--    - Only records where sub_region_1 is empty string
--    - Added NULL check for robustness
--
-- 3. Type Conversions (M: Table.TransformColumnTypes):
--    - Text columns → VARCHAR(MAX)
--    - date → DATE
--    - Percent change columns → BIGINT (Int64.Type)
--
-- 4. Column Count: 12 columns (from original 15)
-- =============================================
```
