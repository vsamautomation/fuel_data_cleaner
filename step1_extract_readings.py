"""
Step 1: Extract Fuel Readings for September 2025
Extract readings for multiple sites across all tanks
"""

import pandas as pd
import requests
from io import StringIO
from datetime import datetime

# Configuration
GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRpva-TXUaQR_6tJoXX2vnSN2ertC5GNxAgssqmXvIhqHBNrscDxSxtiSWbCiiHqAoSHb3SzXDQw_VX/pub?gid=1048590026&single=true&output=csv"
OUTPUT_FILE = "Step1_Sept2025_Readings.xlsx"

# Site definitions: (row_index, site_name)
SITES = [
    (4, 'OLD Morongo'),
    (43, 'NEW Morongo Site #2'),
    (93, 'Fort Independence'),
    (127, 'Campo | Golden Acorn'),
    (248, 'Pechanga | Temecula'),
]


def fetch_data():
    """Fetch Google Sheets data"""
    print("Fetching data from Google Sheets...")
    response = requests.get(GOOGLE_SHEET_URL)
    df = pd.read_csv(StringIO(response.text), header=None, low_memory=False)
    print(f"Data shape: {df.shape}")
    return df


def find_september_columns(df):
    """Find column indices for September 2025"""
    print("\nFinding September 2025 columns...")
    dates_row = df.iloc[0, 6:]
    
    sept_data = []
    for col_idx, date_val in enumerate(dates_row, start=6):
        if pd.notna(date_val):
            try:
                parsed = pd.to_datetime(str(date_val), format='%b-%d-%y', errors='coerce')
                if parsed and parsed.year == 2025 and parsed.month == 9:
                    sept_data.append((col_idx, parsed))
            except:
                pass
    
    print(f"Found {len(sept_data)} days in September 2025")
    print(f"Date range: {sept_data[0][1].date()} to {sept_data[-1][1].date()}")
    return sept_data


def extract_site_readings(df, site_row, site_name, date_columns):
    """Extract readings for a single site"""
    print(f"\n{'='*60}")
    print(f"Extracting: {site_name} (starting at row {site_row})")
    print('='*60)
    
    # Find the READINGS section by looking for "READINGS" label in column 3
    # Then collect product rows that follow until we hit another section
    
    reading_start_row = None
    reading_end_row = None
    
    # Search for READINGS section (within ~20 rows of site header)
    for offset in range(20):
        row_idx = site_row + offset
        if row_idx >= len(df):
            break
        
        section_label = str(df.iloc[row_idx, 3]).strip() if pd.notna(df.iloc[row_idx, 3]) else ""
        
        if "READINGS" in section_label.upper():
            reading_start_row = row_idx + 1  # Data starts next row
            print(f"  Found READINGS section at row {row_idx}")
            break
    
    if reading_start_row is None:
        print(f"  WARNING: Could not find READINGS section for {site_name}")
        return []
    
    # Now find where READINGS section ends (next section like ULLAGE, LOADS, etc.)
    for offset in range(15):
        row_idx = reading_start_row + offset
        if row_idx >= len(df):
            break
        
        section_label = str(df.iloc[row_idx, 3]).strip() if pd.notna(df.iloc[row_idx, 3]) else ""
        
        if any(keyword in section_label.upper() for keyword in ['ULLAGE', 'LOADS', 'CARRIER', 'NOTES']):
            reading_end_row = row_idx
            print(f"  READINGS section ends at row {row_idx} (next section: {section_label})")
            break
    
    if reading_end_row is None:
        reading_end_row = reading_start_row + 10  # Default to 10 rows
    
    # Scan the READINGS section to find all product entries
    records = []
    products_found = {}
    
    for row_idx in range(reading_start_row, reading_end_row):
        if row_idx >= len(df):
            break
        
        # Product is in column 4
        product_cell = df.iloc[row_idx, 4]
        
        if pd.notna(product_cell):
            product = str(product_cell).strip()
            
            # Only process 87, 91, dsl (not totals)
            if product in ['87', '91', 'dsl']:
                # Count how many times we've seen this product
                if product not in products_found:
                    products_found[product] = []
                
                products_found[product].append(row_idx)
                print(f"  Found {product} at row {row_idx}")
    
    # Now extract readings for each date
    for col_idx, date in date_columns:
        for product, row_indices in products_found.items():
            # Create a record with multiple tank readings
            record = {
                'Date': date.strftime('%Y-%m-%d'),
                'Site': site_name,
                'Product': product
            }
            
            # Extract values for each tank (each row is a different tank)
            for tank_num, row_idx in enumerate(row_indices, start=1):
                value = df.iloc[row_idx, col_idx]
                
                # Clean the value
                if pd.notna(value):
                    try:
                        clean_val = str(value).replace(',', '').strip()
                        numeric_val = float(clean_val) if clean_val else None
                        record[f'Tank_{tank_num}_Reading'] = numeric_val
                    except:
                        record[f'Tank_{tank_num}_Reading'] = None
                else:
                    record[f'Tank_{tank_num}_Reading'] = None
            
            records.append(record)
    
    print(f"  Extracted {len(records)} records for {site_name}")
    return records


def main():
    """Main execution"""
    print("="*80)
    print("STEP 1: EXTRACT SEPTEMBER 2025 READINGS")
    print("="*80)
    
    # Step 1: Fetch data
    df = fetch_data()
    
    # Step 2: Find September columns
    sept_cols = find_september_columns(df)
    
    # Step 3: Extract readings for each site
    all_records = []
    
    for site_row, site_name in SITES:
        records = extract_site_readings(df, site_row, site_name, sept_cols)
        all_records.extend(records)
    
    # Step 4: Convert to DataFrame
    print(f"\n{'='*80}")
    print(f"Creating final dataset...")
    df_output = pd.DataFrame(all_records)
    
    # Get all tank columns and sort them
    tank_cols = [col for col in df_output.columns if col.startswith('Tank_')]
    tank_cols_sorted = sorted(tank_cols, key=lambda x: int(x.split('_')[1]))
    
    # Reorder columns
    base_cols = ['Date', 'Site', 'Product']
    df_output = df_output[base_cols + tank_cols_sorted]
    
    # Sort by Date, Site, Product
    df_output = df_output.sort_values(['Date', 'Site', 'Product'])
    
    print(f"Total records: {len(df_output)}")
    print(f"Columns: {list(df_output.columns)}")
    
    # Step 5: Export to Excel
    print(f"\nExporting to: {OUTPUT_FILE}")
    df_output.to_excel(OUTPUT_FILE, index=False, sheet_name='Sept2025_Readings')
    
    # Display sample
    print("\n" + "="*80)
    print("SAMPLE OUTPUT (First 15 rows):")
    print("="*80)
    print(df_output.head(15).to_string(index=False))
    
    print("\n" + "="*80)
    print("SUCCESS!")
    print("="*80)
    print(f"Output saved to: {OUTPUT_FILE}")
    print(f"Total sites: {len(SITES)}")
    print(f"Date range: September 1-30, 2025")
    print(f"Total records: {len(df_output)}")
    
    return df_output


if __name__ == "__main__":
    df_result = main()
