"""
Step 4: Extract INV SETTINGS (Desired Tank Levels)
Extract the desired inventory levels for efficient operation at each site
"""

import pandas as pd
import requests
from io import StringIO

# Configuration
GOOGLE_SHEET_URL = ""
OUTPUT_FILE = "Step4_Inv_Settings.xlsx"

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


def extract_site_inv_settings(df, site_row, site_name):
    """Extract inventory settings for a single site"""
    print(f"\n{'='*60}")
    print(f"Extracting: {site_name} (starting at row {site_row})")
    print('='*60)
    
    # Strategy:
    # 1. Find the row with "INV. SETTING" in column 1
    # 2. Extract all rows below it until we hit "TANK SIZE"
    # 3. For each row, get desired level from column 1 and product from column 4
    # 4. Skip rows where product contains "total"
    
    inv_setting_row = None
    
    # Find INV. SETTING label in column 1
    for offset in range(20):
        row_idx = site_row + offset
        if row_idx >= len(df):
            break
        
        label = str(df.iloc[row_idx, 1]).strip() if pd.notna(df.iloc[row_idx, 1]) else ""
        
        if "INV. SETTING" in label.upper() or "INV SETTING" in label.upper():
            inv_setting_row = row_idx
            print(f"  Found INV. SETTING label at row {row_idx}")
            break
    
    if inv_setting_row is None:
        print(f"  WARNING: Could not find INV. SETTING label for {site_name}")
        return []
    
    # Now extract all inventory settings from rows below until we hit TANK SIZE
    records = []
    products_found = {}
    
    for row_idx in range(inv_setting_row + 1, inv_setting_row + 20):
        if row_idx >= len(df):
            break
        
        # Check if we've hit TANK SIZE section (end of inv settings section)
        col1_label = str(df.iloc[row_idx, 1]).strip() if pd.notna(df.iloc[row_idx, 1]) else ""
        
        if "TANK SIZE" in col1_label.upper():
            print(f"  INV. SETTING section ends at row {row_idx} (TANK SIZE found)")
            break
        
        # Get desired level from column 1 (column B)
        desired_level = df.iloc[row_idx, 1]
        
        # Get product from column 4 (column E)
        product_cell = df.iloc[row_idx, 4]
        
        # Only process if both desired level and product exist
        if pd.notna(desired_level) and pd.notna(product_cell):
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
                    clean_val = str(desired_level).replace(',', '').strip()
                    level_val = float(clean_val) if clean_val else None
                    
                    if level_val and level_val > 0:
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
                                'Desired_Level': level_val,
                                'Is_Total': True
                            })
                            print(f"  Found {base_product} TOTAL: {level_val:,.0f} (row {row_idx})")
                        else:
                            # For individual tank rows
                            tank_num = len(products_found[base_product]) + 1
                            products_found[base_product].append(level_val)
                            
                            records.append({
                                'Site': site_name,
                                'Product': base_product,
                                'Tank_Number': tank_num,
                                'Desired_Level': level_val,
                                'Is_Total': False
                            })
                            print(f"  Found {base_product} Tank {tank_num}: {level_val:,.0f} (row {row_idx})")
                except:
                    pass
    
    print(f"  Extracted {len(records)} INV. SETTING records for {site_name}")
    return records


def main():
    """Main execution"""
    print("="*80)
    print("STEP 4: EXTRACT INV SETTINGS (DESIRED TANK LEVELS)")
    print("="*80)
    
    # Step 1: Fetch data
    df = fetch_data()
    
    # Step 2: Extract inventory settings for each site
    all_records = []
    
    for site_row, site_name in SITES:
        records = extract_site_inv_settings(df, site_row, site_name)
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
    df_output.to_excel(OUTPUT_FILE, index=False, sheet_name='Inv_Settings')
    
    # Display full output
    print("\n" + "="*80)
    print("COMPLETE INV SETTINGS REFERENCE:")
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
                total_desired = product_data['Desired_Level'].sum()
                tank_count = len(product_data)
                print(f"  {product}: {tank_count} tank(s), Total desired level: {total_desired:,.0f} gallons")
    
    print("\n" + "="*80)
    print("SUCCESS!")
    print("="*80)
    print(f"Output saved to: {OUTPUT_FILE}")
    print(f"Total sites: {len(SITES)}")
    print(f"Total settings: {len(df_output)}")
    
    print("\n" + "="*80)
    print("ALL 4 DATASETS COMPLETE!")
    print("="*80)
    print("1. Step1_Sept2025_Readings.xlsx    - Daily tank readings")
    print("2. Step2_Sept2025_Ullage.xlsx      - Daily ullage (empty space)")
    print("3. Step3_Tank_Sizes.xlsx            - Tank capacities")
    print("4. Step4_Inv_Settings.xlsx          - Desired inventory levels")
    print("\nReady for Excel forecasting! 🎯")
    
    return df_output


if __name__ == "__main__":
    df_result = main()
