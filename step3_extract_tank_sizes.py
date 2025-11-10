"""
Step 3: Extract Tank Sizes
Extract tank capacity information for each site and product
Tank Size - Reading = Ullage
"""

import pandas as pd
import requests
from io import StringIO

# Configuration
GOOGLE_SHEET_URL = ""
OUTPUT_FILE = "Step3_Tank_Sizes.xlsx"

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


def extract_site_tank_sizes(df, site_row, site_name):
    """Extract tank sizes for a single site"""
    print(f"\n{'='*60}")
    print(f"Extracting: {site_name} (starting at row {site_row})")
    print('='*60)
    
    # Strategy:
    # 1. Find the row with "TANK SIZE" label in column 1
    # 2. Extract all rows below it until we hit "SALES" or "SALES (projected)"
    # 3. For each row, get tank size from column 1 and product from column 4
    # 4. Skip rows where product contains "total"
    
    tank_size_row = None
    
    # Find TANK SIZE label in column 1
    for offset in range(40):
        row_idx = site_row + offset
        if row_idx >= len(df):
            break
        
        label = str(df.iloc[row_idx, 1]).strip() if pd.notna(df.iloc[row_idx, 1]) else ""
        
        if "TANK SIZE" in label.upper():
            tank_size_row = row_idx
            print(f"  Found TANK SIZE label at row {row_idx}")
            break
    
    if tank_size_row is None:
        print(f"  WARNING: Could not find TANK SIZE label for {site_name}")
        return []
    
    # Now extract all tank sizes from rows below until we hit SALES
    records = []
    products_found = {}
    
    for row_idx in range(tank_size_row + 1, tank_size_row + 20):
        if row_idx >= len(df):
            break
        
        # Check if we've hit SALES section (end of tank size section)
        col1_label = str(df.iloc[row_idx, 1]).strip() if pd.notna(df.iloc[row_idx, 1]) else ""
        col3_label = str(df.iloc[row_idx, 3]).strip() if pd.notna(df.iloc[row_idx, 3]) else ""
        
        if "SALES" in col1_label.upper() or "SALES" in col3_label.upper():
            print(f"  Tank size section ends at row {row_idx} (SALES section found)")
            break
        
        # Get tank size from column 1 (column B)
        tank_size = df.iloc[row_idx, 1]
        
        # Get product from column 4 (column E)
        product_cell = df.iloc[row_idx, 4]
        
        # Only process if both tank size and product exist
        if pd.notna(tank_size) and pd.notna(product_cell):
            product = str(product_cell).strip()
            
            # Extract base product (87, 91, dsl) - handle "87 total", etc.
            base_product = None
            if "87" in product:
                base_product = '87'
            elif "91" in product:
                base_product = '91'
            elif "dsl" in product.lower():
                base_product = 'dsl'
            
            if base_product:
                try:
                    clean_val = str(tank_size).replace(',', '').strip()
                    size_val = float(clean_val) if clean_val else None
                    
                    if size_val and size_val > 0:
                        # Check if this is a "total" row
                        is_total = "total" in product.lower()
                        
                        # Track product occurrences for tank numbering
                        if base_product not in products_found:
                            products_found[base_product] = []
                        
                        if is_total:
                            # For total rows, use Tank_Number = 0 to indicate aggregate
                            records.append({
                                'Site': site_name,
                                'Product': base_product,
                                'Tank_Number': 0,  # 0 = Total/Aggregate
                                'Tank_Size': size_val,
                                'Is_Total': True
                            })
                            print(f"  Found {base_product} TOTAL: {size_val:,.0f} (row {row_idx})")
                        else:
                            # For individual tank rows
                            tank_num = len(products_found[base_product]) + 1
                            products_found[base_product].append(size_val)
                            
                            records.append({
                                'Site': site_name,
                                'Product': base_product,
                                'Tank_Number': tank_num,
                                'Tank_Size': size_val,
                                'Is_Total': False
                            })
                            print(f"  Found {base_product} Tank {tank_num}: {size_val:,.0f} (row {row_idx})")
                except:
                    pass
    
    print(f"  Extracted {len(records)} tank size records for {site_name}")
    return records


def main():
    """Main execution"""
    print("="*80)
    print("STEP 3: EXTRACT TANK SIZES")
    print("="*80)
    
    # Step 1: Fetch data
    df = fetch_data()
    
    # Step 2: Extract tank sizes for each site
    all_records = []
    
    for site_row, site_name in SITES:
        records = extract_site_tank_sizes(df, site_row, site_name)
        all_records.extend(records)
    
    # Step 3: Convert to DataFrame
    print(f"\n{'='*80}")
    print(f"Creating final dataset...")
    df_output = pd.DataFrame(all_records)
    
    # Sort by Site, Product, Tank_Number
    df_output = df_output.sort_values(['Site', 'Product', 'Tank_Number'])
    
    print(f"Total records: {len(df_output)}")
    print(f"Columns: {list(df_output.columns)}")
    
    # Step 4: Export to Excel
    print(f"\nExporting to: {OUTPUT_FILE}")
    df_output.to_excel(OUTPUT_FILE, index=False, sheet_name='Tank_Sizes')
    
    # Display full output (it's small enough)
    print("\n" + "="*80)
    print("COMPLETE TANK SIZE REFERENCE:")
    print("="*80)
    print(df_output.to_string(index=False))
    
    # Summary by site
    print("\n" + "="*80)
    print("SUMMARY BY SITE:")
    print("="*80)
    for site in df_output['Site'].unique():
        site_data = df_output[df_output['Site'] == site]
        print(f"\n{site}:")
        for product in ['87', '91', 'dsl']:
            product_data = site_data[site_data['Product'] == product]
            if len(product_data) > 0:
                total_capacity = product_data['Tank_Size'].sum()
                tank_count = len(product_data)
                print(f"  {product}: {tank_count} tank(s), Total capacity: {total_capacity:,.0f} gallons")
    
    print("\n" + "="*80)
    print("SUCCESS!")
    print("="*80)
    print(f"Output saved to: {OUTPUT_FILE}")
    print(f"Total sites: {len(SITES)}")
    print(f"Total tanks: {len(df_output)}")
    
    return df_output


if __name__ == "__main__":
    df_result = main()
