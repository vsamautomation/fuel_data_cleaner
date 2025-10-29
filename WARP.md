# WARP.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## Project Overview

This is a **Fuel Data Extractor** that fetches fuel inventory data from a published Google Sheets document and transforms it into clean, analysis-ready CSV files. The project extracts readings, tank sizes, inventory settings, and actual sales data for 26+ fuel sites spanning 672 dates (March 2024 - January 2026).

### Core Purpose
- Fetch real-time data from Google Sheets (no manual downloads)
- Dynamically identify all fuel sites without hardcoding
- Extract 4 types of data: readings, ullage, tank sizes, and inventory settings
- Output tidy/long-format CSV files for Excel analysis

## Key Commands

### Running the Extractor

**Primary command (uses default current directory):**
```bash
python3 extract_fuel_data.py
```

**With custom output directory:**
```bash
python3 extract_fuel_data.py -o /path/to/output
# or short form:
python3 extract_fuel_data.py --output ~/Desktop/fuel_data
```

**Via launcher scripts (for end users):**
- macOS: `./RUN_EXTRACTOR.command` (double-click)
- Windows: `RUN_EXTRACTOR.bat` (double-click)

### Testing Individual Extraction Steps

The project has legacy step-by-step extraction scripts (now superseded by the unified extractor):

```bash
# Step 1: Extract readings only (September 2025)
python3 step1_extract_readings.py

# Step 2: Extract ullage (September 2025)
python3 step2_extract_ullage.py

# Step 3: Extract tank sizes
python3 step3_extract_tank_sizes.py

# Step 4: Extract inventory settings
python3 step4_extract_inv_settings.py
```

**Note:** These individual scripts are legacy and extract limited date ranges. Use `extract_fuel_data.py` for complete extraction.

### Testing Site Identification

```bash
# Test the site identifier module independently
python3 site_identifier.py
```

### Dependencies

```bash
# Install required packages
pip install pandas requests

# For building standalone executables (optional)
pip install pyinstaller
```

## Architecture

### Main Components

1. **`extract_fuel_data.py`** - Unified extraction script
   - Orchestrates the entire extraction pipeline
   - Uses `site_identifier.py` for dynamic site discovery
   - Extracts all 4 data types for all sites and dates
   - Outputs 4 CSV files: `fuel_readings.csv`, `tank_sizes.csv`, `inv_settings.csv`, `sales_actual.csv`

2. **`site_identifier.py`** - Site discovery module
   - Finds sites by locating "INV. SETTING" labels in column 1
   - Extracts site names from the row above each label
   - Validates sites have expected data sections
   - Returns list of `(row_index, site_name)` tuples

3. **Legacy extraction scripts** - Step-by-step extractors
   - `step1_extract_readings.py` - Tank readings
   - `step2_extract_ullage.py` - Ullage (empty space) readings
   - `step3_extract_tank_sizes.py` - Tank capacities
   - `step4_extract_inv_settings.py` - Desired inventory levels
   - These scripts contain the original extraction logic now integrated into the main script

4. **Launcher scripts** - User-friendly execution
   - `RUN_EXTRACTOR.command` - macOS launcher with Python checks
   - `RUN_EXTRACTOR.bat` - Windows launcher with Python checks

### Data Flow

```
Google Sheets (published CSV) 
    ↓
fetch_data() → pandas DataFrame
    ↓
identify_sites() → List of (row, site_name)
    ↓
get_all_dates() → List of (col_idx, date) up to today
    ↓
For each site:
    ├─ extract_site_readings()      → readings records
    ├─ extract_site_tank_sizes()    → tank size records
    ├─ extract_site_inv_settings()  → inv settings records
    └─ extract_site_sales_actual()  → sales records
    ↓
Convert to DataFrames → Export 4 CSV files
```

### Google Sheets Structure

The source data has a specific structure that the extractor depends on:
- **Row 0**: Date headers starting at column 6 (format: "Mar-01-24")
- **Column 1 (B)**: Site names, section labels (INV. SETTING, TANK SIZE), and numeric values
- **Column 3 (D)**: Section labels (READINGS, ULLAGE, LOADS, SALES)
- **Column 4 (E)**: Product identifiers (87, 91, dsl, with optional "total" suffix)
- **Column 6+**: Date columns with numeric values

Each site follows this pattern:
1. Site name row
2. INV. SETTING section (desired levels)
3. TANK SIZE section (capacities)
4. READINGS section (AM readings)
5. ULLAGE section (empty space)
6. LOADS section (fuel deliveries)
7. SALES (projected) section
8. SALES (actual) section

### Extraction Logic Patterns

**Site Discovery:**
- Search for "INV. SETTING" or "INV SETTING" in column 1
- Site name is always in row directly above
- Clean names by removing leading numbers/letters (e.g., "1a OLD Morongo" → "OLD Morongo")

**Section Extraction:**
1. Find section start by searching for label in column 3 or 1
2. Find section end by looking for next section keyword
3. Scan rows for product identifiers in column 4
4. Extract values from date columns for each product row

**Product Identification:**
- Base products: `87`, `91`, `dsl`
- Products can have "total" suffix for aggregate values
- Track tank numbers by counting occurrences of each base product

**Date Filtering:**
- Only extract dates up to today (not future dates)
- Parse dates using format: `%b-%d-%y` (e.g., "Sep-15-25")
- Store in ISO format: `YYYY-MM-DD`

### Output Format

All CSV files use **long/tidy format** for easy analysis:

**fuel_readings.csv:**
- Columns: Date, Site, Product, Tank_1_Reading, Tank_2_Reading, ...
- One row per date/site/product combination
- Variable number of tank columns based on site configuration

**tank_sizes.csv:**
- Columns: Site, Product, Tank_Number, Tank_Size, Is_Total
- Tank_Number=0 indicates total/aggregate
- Is_Total=True for aggregate rows

**inv_settings.csv:**
- Columns: Site, Product, Tank_Number, Desired_Level, Is_Total
- Same structure as tank_sizes.csv

**sales_actual.csv:**
- Columns: Date, Site, Product, Sales_Actual, Is_Total
- Daily sales by site and product

## Development Guidelines

### When Modifying Extraction Logic

1. **Test with site_identifier.py first** - Ensure site discovery still works
2. **Check section boundaries** - Google Sheets structure may vary between sites
3. **Validate date filtering** - Dates beyond today should be excluded
4. **Handle missing data gracefully** - Some sites have incomplete sections
5. **Preserve tidy format** - Keep output in long format for analysis

### Common Patterns

**Finding a section:**
```python
section_start_row = None
for offset in range(20):  # Scan reasonable range
    row_idx = site_row + offset
    if row_idx >= len(df):
        break
    label = str(df.iloc[row_idx, col]).strip() if pd.notna(df.iloc[row_idx, col]) else ""
    if "SECTION_NAME" in label.upper():
        section_start_row = row_idx
        break
```

**Extracting products:**
```python
products_found = {}
for row_idx in range(section_start, section_end):
    product_cell = df.iloc[row_idx, 4]
    if pd.notna(product_cell):
        product = str(product_cell).strip()
        if product in ['87', '91', 'dsl']:
            if product not in products_found:
                products_found[product] = []
            products_found[product].append(row_idx)
```

**Cleaning numeric values:**
```python
clean_val = str(value).replace(',', '').strip()
numeric_val = float(clean_val) if clean_val else None
```

### Data Source

The Google Sheet URL is hardcoded in the scripts:
```python
GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRpva-TXUaQR_6tJoXX2vnSN2ertC5GNxAgssqmXvIhqHBNrscDxSxtiSWbCiiHqAoSHb3SzXDQw_VX/pub?gid=1048590026&single=true&output=csv"
```

This must be a **published Google Sheets CSV** endpoint. To change the data source, update this constant in all relevant scripts.

### File Locations

- **Output files**: By default saved to current directory (customizable via `-o` flag)
- **Test output**: `test_date_filter/`, `test_executable_output/`, `test_sales_output/`
- **Build artifacts**: `build/`, `dist/`, `__pycache__/` (all gitignored)

### Python Requirements

- Python 3.7+
- pandas (data manipulation)
- requests (HTTP requests to Google Sheets)

No virtual environment setup is documented - dependencies are installed globally.
