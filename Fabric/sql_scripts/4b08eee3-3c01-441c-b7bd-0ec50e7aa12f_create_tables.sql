```sql
----------------------------------------------------------------------------------------------------
-- Dataset: COVID Bakeoff PBIR
-- Workspace: Auto_DP
-- Generated: T-SQL Migration Script for Power BI Dataset
-- Purpose: Dimensional model migration to SQL Server for Power BI dataset
----------------------------------------------------------------------------------------------------

----------------------------------------------------------------------------------------------------
-- Table: States
-- Description: Dimension table containing US state information with demographic and climate data.
--              Serves as a dimension for US-specific COVID analysis.
-- Usage in Power BI: 0 measures, 2 relationships
----------------------------------------------------------------------------------------------------
CREATE TABLE [States] (
    [Average Temperature ] NUMERIC(18,2), -- The average temperature recorded for the state (note: trailing space in column name is intentional)
    [Flag] VARCHAR(255),                   -- Flag emoji or indicator associated with the state
    [Population] NUMERIC(18,2),            -- The total population of the state
    [State] VARCHAR(255)                   -- The name of the state (links to Cases per US State and Lats tables)
);

----------------------------------------------------------------------------------------------------
-- Table: OWID COVID data
-- Description: Fact table containing COVID-19 data from Our World in Data (OWID).
--              Primary source for global COVID case tracking and analysis.
-- Usage in Power BI: 10 measures, 2 relationships
----------------------------------------------------------------------------------------------------
CREATE TABLE [OWID COVID data] (
    [date] DATETIME2(6),      -- Date of the COVID data record (FK to Dates.Date)
    [iso_code] VARCHAR(255),  -- ISO country code (FK to Countries.ISO)
    [New cases] BIGINT        -- Number of new COVID cases reported on this date
);

----------------------------------------------------------------------------------------------------
-- Table: CGRT Mandates
-- Description: Contains COVID-19 Government Response Tracker (CGRT) mandate information.
--              Links government mandates to countries for policy analysis.
-- Usage in Power BI: 0 measures, 1 relationships
----------------------------------------------------------------------------------------------------
CREATE TABLE [CGRT Mandates] (
    [CountryName] VARCHAR(255) -- Country name for the mandate record (FK to Countries.Country)
);

----------------------------------------------------------------------------------------------------
-- Table: Cases per US State
-- Description: Fact table tracking COVID-19 cases and vaccination data at the US state level.
--              Primary source for US state-level COVID analysis and vaccination tracking.
-- Usage in Power BI: 14 measures, 2 relationships
----------------------------------------------------------------------------------------------------
CREATE TABLE [Cases per US State] (
    [Date] DATETIME2(6),                              -- Date of the case record (FK to Dates.Date)
    [Incremental cases] BIGINT,                       -- Number of new cases reported on this date for the state
    [People fully vaccinated per hundred] NUMERIC(18,2), -- Percentage of population fully vaccinated per 100 people
    [State] VARCHAR(255),                             -- US State name (FK to States.State)
    [people_vaccinated_per_hundred] NUMERIC(18,2),    -- Percentage of population with at least one vaccine dose per 100 people
    [total_distributed] BIGINT                        -- Total number of vaccine doses distributed to the state
);

----------------------------------------------------------------------------------------------------
-- Table: Cases per country
-- Description: Fact table tracking COVID-19 cases at the country level.
--              Provides country-level case data for global analysis.
-- Usage in Power BI: 0 measures, 2 relationships
----------------------------------------------------------------------------------------------------
CREATE TABLE [Cases per country] (
    [Country] VARCHAR(255),    -- Country name (FK to Countries.Country)
    [Date] DATETIME2(6),       -- Date of the case record (FK to Dates.Date)
    [IncrementalCases] BIGINT  -- Number of new cases reported on this date for the country
);

----------------------------------------------------------------------------------------------------
-- Table: Govt Measures
-- Description: Fact table containing government measures and interventions related to COVID-19.
--              Tracks implementation dates and entry dates of various policy measures.
-- Usage in Power BI: 1 measures, 3 relationships
----------------------------------------------------------------------------------------------------
CREATE TABLE [Govt Measures] (
    [Date implemented] DATETIME2(6), -- Date when the government measure was implemented (FK to Dates.Date)
    [Entry date] DATETIME2(6),       -- Date when the measure was entered into the system (FK to LocalDateTable)
    [ISO] VARCHAR(255)               -- ISO country code for the measure (FK to Countries.ISO)
);

----------------------------------------------------------------------------------------------------
-- Table: Days with restrictions grouped
-- Description: Dimension table containing categorized COVID-19 restriction types by country.
--              Groups various restriction measures for comparative analysis.
-- Usage in Power BI: 0 measures, 1 relationships
----------------------------------------------------------------------------------------------------
CREATE TABLE [Days with restrictions grouped] (
    [Cancelling public events] VARCHAR(255),      -- Status or level of public event cancellations
    [CountryCode] VARCHAR(255),                   -- ISO country code (FK to Countries.ISO)
    [Domestic travel restrictions] VARCHAR(255),  -- Status or level of domestic travel restrictions
    [Face coverings required] VARCHAR(255),       -- Status or level of face covering requirements
    [International travel controls] VARCHAR(255), -- Status or level of international travel controls
    [Public transport closures] VARCHAR(255),     -- Status or level of public transport closures
    [Restrictions on gathering] VARCHAR(255),     -- Status or level of gathering restrictions
    [School closures] VARCHAR(255),               -- Status or level of school closures
    [Stay at home requirements] VARCHAR(255),     -- Status or level of stay-at-home orders
    [Workplace closures] VARCHAR(255)             -- Status or level of workplace closures
);

----------------------------------------------------------------------------------------------------
-- Table: GDP History
-- Description: Dimension table containing historical GDP data by country.
--              Provides economic context for COVID impact analysis.
-- Usage in Power BI: 0 measures, 1 relationships
----------------------------------------------------------------------------------------------------
CREATE TABLE [GDP History] (
    [% change] NUMERIC(18,2), -- Percentage change in GDP
    [ISO] VARCHAR(255),       -- ISO country code (FK to Countries.ISO)
    [Year] BIGINT             -- Year of the GDP record
);

----------------------------------------------------------------------------------------------------
-- Table: DateTableTemplate_3fa67ac2-0afb-4cc0-9c50-279baae0411c
-- Description: Auto-generated date table template from Power BI.
--              Template table for date dimension generation (currently empty).
-- Usage in Power BI: 0 measures, 0 relationships
----------------------------------------------------------------------------------------------------
CREATE TABLE [DateTableTemplate_3fa67ac2-0afb-4cc0-9c50-279baae0411c] (
    -- No columns defined in source dataset
    [PlaceholderColumn] VARCHAR(1) -- Placeholder column as SQL tables require at least one column
);

----------------------------------------------------------------------------------------------------
-- Table: Countries
-- Description: Primary dimension table containing country master data.
--              Central dimension for geographic analysis with demographic information.
-- Usage in Power BI: 2 measures, 7 relationships
----------------------------------------------------------------------------------------------------
CREATE TABLE [Countries] (
    [Continent] VARCHAR(255), -- Continent where the country is located
    [Country] VARCHAR(255),   -- Country name (links to multiple fact tables)
    [Flag] VARCHAR(255),      -- Flag emoji or indicator for the country
    [ISO] VARCHAR(255),       -- ISO country code (primary identifier, links to multiple fact tables)
    [Population] BIGINT,      -- Total population of the country
    [REGION] VARCHAR(255)     -- Geographic region classification for the country
);

----------------------------------------------------------------------------------------------------
-- Table: Lats
-- Description: Dimension table containing geographical coordinates for US states.
--              Provides latitude data for mapping and spatial analysis.
-- Usage in Power BI: 0 measures, 1 relationships
----------------------------------------------------------------------------------------------------
CREATE TABLE [Lats] (
    [State] VARCHAR(255) -- US State name (FK to States.State)
);

----------------------------------------------------------------------------------------------------
-- Table: Dates
-- Description: Primary date dimension table for time-based analysis.
--              Central date table linking multiple fact tables for temporal analysis.
-- Usage in Power BI: 0 measures, 4 relationships
----------------------------------------------------------------------------------------------------
CREATE TABLE [Dates] (
    [Date] DATETIME2(6) -- Date value (links to multiple fact tables)
);

----------------------------------------------------------------------------------------------------
-- Table: Days with restrictions
-- Description: Fact/dimension table tracking days when COVID restrictions were in place by country.
--              Used for analyzing duration and patterns of restrictions.
-- Usage in Power BI: 0 measures, 1 relationships
----------------------------------------------------------------------------------------------------
CREATE TABLE [Days with restrictions] (
    [CountryCode] VARCHAR(255) -- ISO country code (FK to Countries.ISO)
);

----------------------------------------------------------------------------------------------------
-- Table: LocalDateTable_88205f4b-f7a1-45b0-926f-f9aeda622848
-- Description: Auto-generated local date table from Power BI for specific date column.
--              System-generated date table for Govt Measures.Entry date column.
-- Usage in Power BI: 0 measures, 1 relationships
----------------------------------------------------------------------------------------------------
CREATE TABLE [LocalDateTable_88205f4b-f7a1-45b0-926f-f9aeda622848] (
    [Date] DATETIME2(6) -- Date value for auto-generated date hierarchy
);

----------------------------------------------------------------------------------------------------
-- End of Migration Script
----------------------------------------------------------------------------------------------------
```