"""
Complete Fuel Data Extraction
Extracts READINGS, LOADS, TANK SIZES, INV SETTINGS, and SALES (actual) from Google Sheets
Outputs 5 CSV files in long format (tidy data)

Usage:
    python extract_fuel_data.py
    python extract_fuel_data.py --output /path/to/output/folder
"""

import pandas as pd
import requests
from io import StringIO
from datetime import datetime
import argparse
import os

from validators import base16
from site_identifier import identify_sites

# Configuration
GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRpva-TXUaQR_6tJoXX2vnSN2ertC5GNxAgssqmXvIhqHBNrscDxSxtiSWbCiiHqAoSHb3SzXDQw_VX/pub?gid=1048590026&single=true&output=csv"

# Default output location (current directory)
DEFAULT_OUTPUT_DIR = "."


def fetch_data():
    """Fetch Google Sheets data"""
    print("Fetching data from Google Sheets...")
    response = requests.get(GOOGLE_SHEET_URL, timeout=30)
    df = pd.read_csv(StringIO(response.text), header=None, low_memory=False)
    print(f"✓ Data shape: {df.shape}")
    return df


def get_all_dates(df, start_col=6):
    """Extract all date columns from the sheet (up to today only)"""
    print("\nExtracting all dates...")
    dates_row = df.iloc[0, start_col:]
    
    today = pd.Timestamp.now().normalize()  # Get today's date at midnight
    
    date_data = []
    for col_idx, date_val in enumerate(dates_row, start=start_col):
        if pd.notna(date_val):
            try:
                parsed = pd.to_datetime(str(date_val), format='%b-%d-%y', errors='coerce')
                if parsed and parsed <= today:  # Only include dates up to today
                    date_data.append((col_idx, parsed))
            except:
                pass
    
    print(f"✓ Found {len(date_data)} dates (up to today)")
    if date_data:
        print(f"  Date range: {date_data[0][1].date()} to {date_data[-1][1].date()}")
    return date_data


def extract_site_readings(df, site_row, site_name, date_columns):
    """Extract readings for a single site"""
    print(f"  Extracting READINGS for {site_name}...")
    
    # Find READINGS section
    reading_start_row = None
    reading_end_row = None
    
    for offset in range(20):
        row_idx = site_row + offset
        if row_idx >= len(df):
            break
        
        section_label = str(df.iloc[row_idx, 3]).strip() if pd.notna(df.iloc[row_idx, 3]) else ""
        
        if "READINGS" in section_label.upper():
            reading_start_row = row_idx + 1
            break
    
    if reading_start_row is None:
        return []
    
    # Find end of READINGS section
    for offset in range(15):
        row_idx = reading_start_row + offset
        if row_idx >= len(df):
            break
        
        section_label = str(df.iloc[row_idx, 3]).strip() if pd.notna(df.iloc[row_idx, 3]) else ""
        
        if any(keyword in section_label.upper() for keyword in ['ULLAGE', 'LOADS', 'CARRIER', 'NOTES']):
            reading_end_row = row_idx
            break
    
    if reading_end_row is None:
        reading_end_row = reading_start_row + 10
    
    # Scan READINGS section
    records = []
    products_found = {}
    
    for row_idx in range(reading_start_row, reading_end_row):
        if row_idx >= len(df):
            break
        
        product_cell = df.iloc[row_idx, 4]
        
        if pd.notna(product_cell):
            product = str(product_cell).strip()
            
            if product in ['87', '88', '91', 'dsl', 'racing', 'red']:
                if product not in products_found:
                    products_found[product] = []
                
                products_found[product].append(row_idx)
    
    # Extract readings for each date
    for col_idx, date in date_columns:
        for product, row_indices in products_found.items():
            record = {
                'Date': date.strftime('%Y-%m-%d'),
                'Site': site_name,
                'Product': product
            }
            
            for tank_num, row_idx in enumerate(row_indices, start=1):
                value = df.iloc[row_idx, col_idx]
                
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
    
    return records


def extract_site_loads(df, site_row, site_name, date_columns):
    """Extract loads (fuel deliveries) for a single site"""
    print(f"  Extracting LOADS for {site_name}...")
    
    # First, find ULLAGE section
    ullage_row = None
    for offset in range(30):
        row_idx = site_row + offset
        if row_idx >= len(df):
            break
        
        section_label = str(df.iloc[row_idx, 3]).strip() if pd.notna(df.iloc[row_idx, 3]) else ""
        
        if "ULLAGE" in section_label.upper():
            ullage_row = row_idx
            break
    
    if ullage_row is None:
        return []
    
    # Now find LOADS section AFTER ullage
    loads_start_row = None
    loads_end_row = None
    
    for offset in range(1, 20):  # Start searching after ullage
        row_idx = ullage_row + offset
        if row_idx >= len(df):
            break
        
        section_label = str(df.iloc[row_idx, 3]).strip() if pd.notna(df.iloc[row_idx, 3]) else ""
        
        if "LOADS" in section_label.upper():
            loads_start_row = row_idx
            break
    
    if loads_start_row is None:
        return []
    
    # Find end of LOADS section
    for offset in range(1, 15):
        row_idx = loads_start_row + offset
        if row_idx >= len(df):
            break
        
        section_label = str(df.iloc[row_idx, 3]).strip() if pd.notna(df.iloc[row_idx, 3]) else ""
        col1_label = str(df.iloc[row_idx, 1]).strip() if pd.notna(df.iloc[row_idx, 1]) else ""
        
        if any(keyword in section_label.upper() for keyword in ['SALES', 'CARRIER', 'NOTES']) or \
           any(keyword in col1_label.upper() for keyword in ['SALES', 'CARRIER']):
            loads_end_row = row_idx
            break
    
    if loads_end_row is None:
        loads_end_row = loads_start_row + 10
    
    # Scan LOADS section - get product rows
    records = []
    products_found = {}
    
    for row_idx in range(loads_start_row, loads_end_row):
        if row_idx >= len(df):
            break
        
        product_cell = df.iloc[row_idx, 4]
        
        if pd.notna(product_cell):
            product = str(product_cell).strip()
            
            # Get base product (87, 88, racing, red 91, dsl)
            base_product = None
            
            if "87" in product:
                base_product = product
            elif "88" in product:
                base_product = product
            elif "91" in product:
                base_product = product
            elif "dsl" in product.lower():
                base_product = product
            elif "racing" in product.lower():
                base_product = product
            elif "red" in product.lower():
                base_product = product
            
            # Capture all product rows (prefer total if exists, otherwise take the row)
            if base_product:
                is_total = "total" in product.lower()
                # If we haven't seen this product yet, or this is a total row, store it
                if base_product not in products_found or is_total:
                    products_found[base_product] = row_idx
    
    # Extract loads for each date (only totals)
    for col_idx, date in date_columns:
        for product, row_idx in products_found.items():
            value = df.iloc[row_idx, col_idx]
            
            if pd.notna(value):
                try:
                    clean_val = str(value).replace(',', '').strip()
                    load_val = float(clean_val) if clean_val else None
                    
                    if load_val is not None:
                        records.append({
                            'Date': date.strftime('%Y-%m-%d'),
                            'Site': site_name,
                            'Product': product,
                            'Load_Total': load_val
                        })
                except:
                    pass
    
    return records


def extract_site_tank_sizes(df, site_row, site_name):
    """Extract tank sizes for a single site"""
    print(f"  Extracting TANK SIZES for {site_name}...")
    
    # Find TANK SIZE label
    tank_size_row = None
    
    for offset in range(40):
        row_idx = site_row + offset
        if row_idx >= len(df):
            break
        
        label = str(df.iloc[row_idx, 1]).strip() if pd.notna(df.iloc[row_idx, 1]) else ""
        
        if "TANK SIZE" in label.upper():
            tank_size_row = row_idx
            break
    
    if tank_size_row is None:
        return []
    
    # Extract tank sizes
    records = []
    products_found = {}
    
    for row_idx in range(tank_size_row + 1, tank_size_row + 20):
        if row_idx >= len(df):
            break
        
        col1_label = str(df.iloc[row_idx, 1]).strip() if pd.notna(df.iloc[row_idx, 1]) else ""
        col3_label = str(df.iloc[row_idx, 3]).strip() if pd.notna(df.iloc[row_idx, 3]) else ""
        
        if "SALES" in col1_label.upper() or "SALES" in col3_label.upper():
            break
        
        tank_size = df.iloc[row_idx, 1]
        product_cell = df.iloc[row_idx, 4]
        
        if pd.notna(tank_size) and pd.notna(product_cell):
            product = str(product_cell).strip()
            
            # Extract base product
            base_product = None
            if "87" in product:
                base_product = product
            elif "88" in product:
                base_product = product
            elif "91" in product:
                base_product = product
            elif "dsl" in product.lower():
                base_product = product
            elif "racing" in product.lower():
                base_product = product
            elif "red" in product.lower():
                base_product = product
            
            if base_product:
                try:
                    clean_val = str(tank_size).replace(',', '').strip()
                    size_val = float(clean_val) if clean_val else None
                    
                    if size_val and size_val > 0:
                        is_total = "total" in product.lower()
                        
                        if base_product not in products_found:
                            products_found[base_product] = []
                        
                        if is_total:
                            records.append({
                                'Site': site_name,
                                'Product': base_product,
                                'Tank_Number': 0,
                                'Tank_Size': size_val,
                                'Is_Total': True
                            })
                        else:
                            tank_num = len(products_found[base_product]) + 1
                            products_found[base_product].append(size_val)
                            
                            records.append({
                                'Site': site_name,
                                'Product': base_product,
                                'Tank_Number': tank_num,
                                'Tank_Size': size_val,
                                'Is_Total': False
                            })
                except:
                    pass
    
    return records


def extract_site_sales_actual(df, site_row, site_name, date_columns):
    """Extract actual sales for a single site"""
    print(f"  Extracting SALES (actual) for {site_name}...")
    
    # Find SALES (actual) section
    sales_start_row = None
    
    for offset in range(40):
        row_idx = site_row + offset
        if row_idx >= len(df):
            break
        
        col1_label = str(df.iloc[row_idx, 1]).strip() if pd.notna(df.iloc[row_idx, 1]) else ""
        col3_label = str(df.iloc[row_idx, 3]).strip() if pd.notna(df.iloc[row_idx, 3]) else ""
        
        # Look for SALES (actual) specifically, not SALES (projected)
        if "SALES" in col1_label.upper() and "ACTUAL" in col1_label.upper():
            sales_start_row = row_idx
            break
        elif "ACTUAL" in col3_label.upper() and "SALES" in col1_label.upper():
            sales_start_row = row_idx
            break
    
    if sales_start_row is None:
        return []
    
    # Extract products in SALES (actual) section
    records = []
    products_found = {}
    
    for row_idx in range(sales_start_row, sales_start_row + 10):
        if row_idx >= len(df):
            break
        
        product_cell = df.iloc[row_idx, 4]
        
        if pd.notna(product_cell):
            product = str(product_cell).strip()
            if "READING" in product.upper():
                # print(f"    Reached end of products at row {row_idx}.")
                break
            # Get base product (87, 88, racing, red, 91, dsl) - include totals
            base_product = None
            is_total = False
            
            if "87" in product:
                base_product = product
                is_total = "total" in product.lower()
            elif "88" in product:
                base_product = product
                is_total = "total" in product.lower()
            elif "91" in product:
                base_product = product
                is_total = "total" in product.lower()
            elif "dsl" in product.lower():
                base_product = product
                is_total = "total" in product.lower()
            elif "racing" in product.lower():
                base_product = product
                is_total = "total" in product.lower()
            elif "red" in product.lower():
                base_product = product
                is_total = "total" in product.lower()


            if base_product:
                products_found[product] = {
                    'row_idx': row_idx,
                    'base_product': base_product,
                    'is_total': is_total
                }
    
    # Extract sales for each date
    for col_idx, date in date_columns:
        for product_key, product_info in products_found.items():
            row_idx = product_info['row_idx']
            value = df.iloc[row_idx, col_idx]
            
            if pd.notna(value):
                try:
                    clean_val = str(value).replace(',', '').strip()
                    sales_val = float(clean_val) if clean_val else None
                    
                    if sales_val is not None:
                        records.append({
                            'Date': date.strftime('%Y-%m-%d'),
                            'Site': site_name,
                            'Product': product_info['base_product'],
                            'Sales_Actual': sales_val,
                            'Is_Total': product_info['is_total']
                        })
                except:
                    pass
    
    return records


def extract_site_inv_settings(df, site_row, site_name):
    """Extract inventory settings for a single site"""
    print(f"  Extracting INV SETTINGS for {site_name}...")
    
    # Find INV SETTING label
    inv_setting_row = None
    
    for offset in range(20):
        row_idx = site_row + offset
        if row_idx >= len(df):
            break
        
        label = str(df.iloc[row_idx, 1]).strip() if pd.notna(df.iloc[row_idx, 1]) else ""
        
        if "INV. SETTING" in label.upper() or "INV SETTING" in label.upper():
            inv_setting_row = row_idx
            break
    
    if inv_setting_row is None:
        return []
    
    # Extract inventory settings
    records = []
    products_found = {}
    
    for row_idx in range(inv_setting_row + 1, inv_setting_row + 20):
        if row_idx >= len(df):
            break
        
        col1_label = str(df.iloc[row_idx, 1]).strip() if pd.notna(df.iloc[row_idx, 1]) else ""
        
        if "TANK SIZE" in col1_label.upper():
            break
        
        desired_level = df.iloc[row_idx, 1]
        product_cell = df.iloc[row_idx, 4]
        
        if pd.notna(desired_level) and pd.notna(product_cell):
            product = str(product_cell).strip()
            
            # Extract base product
            base_product = None
            if "87" in product:
                base_product = product
            elif "88" in product:   
                base_product = product
            elif "91" in product:
                base_product = product
            elif "dsl" in product.lower():
                base_product = product
            elif "racing" in product.lower():
                base_product = product
            elif "red" in product.lower():
                base_product = product
            
            if base_product:
                try:
                    clean_val = str(desired_level).replace(',', '').strip()
                    level_val = float(clean_val) if clean_val else None
                    
                    if level_val and level_val > 0:
                        is_total = "total" in product.lower()
                        
                        if base_product not in products_found:
                            products_found[base_product] = []
                        
                        if is_total:
                            records.append({
                                'Site': site_name,
                                'Product': base_product,
                                'Tank_Number': 0,
                                'Desired_Level': level_val,
                                'Is_Total': True
                            })
                        else:
                            tank_num = len(products_found[base_product]) + 1
                            products_found[base_product].append(level_val)
                            
                            records.append({
                                'Site': site_name,
                                'Product': base_product,
                                'Tank_Number': tank_num,
                                'Desired_Level': level_val,
                                'Is_Total': False
                            })
                except:
                    pass
    
    return records


def main():
    """Main execution"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Extract fuel data from Google Sheets')
    parser.add_argument('--output', '-o', type=str, default=DEFAULT_OUTPUT_DIR,
                        help=f'Output directory for CSV files (default: {DEFAULT_OUTPUT_DIR})')
    args = parser.parse_args()
    
    output_dir = args.output
    
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"✓ Created output directory: {output_dir}")
    
    print("="*80)
    print("COMPLETE FUEL DATA EXTRACTION")
    print("="*80)
    print(f"Output directory: {output_dir}")
    
    # Fetch data
    df = fetch_data()
    
    # Dynamically identify all sites
    print("\n" + "="*80)
    print("IDENTIFYING SITES")
    print("="*80)
    sites = identify_sites(df)
    
    if not sites:
        print("✗ No sites found in the data!")
        return
    
    print(f"\n✓ Will extract data for {len(sites)} sites")
    
    # Get all dates
    all_dates = get_all_dates(df)
    
    # Extract data for all sites
    print(f"\n{'='*80}")
    print("EXTRACTING DATA FOR ALL SITES")
    print('='*80)
    
    all_readings = []
    all_loads = []
    all_tank_sizes = []
    all_inv_settings = []
    all_sales_actual = []
    
    for site_row, site_name in sites:
        print(f"\n{site_name}:")
        
        # Extract readings
        readings = extract_site_readings(df, site_row, site_name, all_dates)
        all_readings.extend(readings)
        print(f"    ✓ {len(readings)} reading records")
        
        # Extract loads
        loads = extract_site_loads(df, site_row, site_name, all_dates)
        all_loads.extend(loads)
        print(f"    ✓ {len(loads)} load records")
        
        # Extract tank sizes
        tank_sizes = extract_site_tank_sizes(df, site_row, site_name)
        all_tank_sizes.extend(tank_sizes)
        print(f"    ✓ {len(tank_sizes)} tank size records")
        
        # Extract inv settings
        inv_settings = extract_site_inv_settings(df, site_row, site_name)
        all_inv_settings.extend(inv_settings)
        print(f"    ✓ {len(inv_settings)} inv setting records")
        
        # Extract sales actual
        sales_actual = extract_site_sales_actual(df, site_row, site_name, all_dates)
        all_sales_actual.extend(sales_actual)
        print(f"    ✓ {len(sales_actual)} sales actual records")
    
    # Convert to DataFrames and export
    print(f"\n{'='*80}")
    print("EXPORTING DATA")
    print('='*80)
    
    # 1. READINGS
    df_readings = pd.DataFrame(all_readings)
    if not df_readings.empty:
        df_readings = df_readings.sort_values(['Date', 'Site', 'Product'])
        readings_file = os.path.join(output_dir, 'fuel_readings.csv')
        df_readings.to_csv(readings_file, index=False)
        print(f"✓ READINGS: {readings_file}")
        print(f"  {len(df_readings)} records | {df_readings['Date'].min()} to {df_readings['Date'].max()}")
    
    # 2. LOADS
    df_loads = pd.DataFrame(all_loads)
    if not df_loads.empty:
        df_loads = df_loads.sort_values(['Date', 'Site', 'Product'])
        loads_file = os.path.join(output_dir, 'fuel_loads.csv')
        df_loads.to_csv(loads_file, index=False)
        print(f"✓ LOADS: {loads_file}")
        print(f"  {len(df_loads)} records | {df_loads['Date'].min()} to {df_loads['Date'].max()}")
    
    # 3. TANK SIZES
    df_tank_sizes = pd.DataFrame(all_tank_sizes)
    if not df_tank_sizes.empty:
        df_tank_sizes = df_tank_sizes.sort_values(['Site', 'Product', 'Tank_Number'])
        tank_sizes_file = os.path.join(output_dir, 'tank_sizes.csv')
        df_tank_sizes.to_csv(tank_sizes_file, index=False)
        print(f"✓ TANK SIZES: {tank_sizes_file}")
        print(f"  {len(df_tank_sizes)} records | {df_tank_sizes[df_tank_sizes['Is_Total']==True].shape[0]} totals")
    
    # 4. INV SETTINGS
    df_inv_settings = pd.DataFrame(all_inv_settings)
    if not df_inv_settings.empty:
        df_inv_settings = df_inv_settings.sort_values(['Site', 'Product', 'Tank_Number'])
        inv_settings_file = os.path.join(output_dir, 'inv_settings.csv')
        df_inv_settings.to_csv(inv_settings_file, index=False)
        print(f"✓ INV SETTINGS: {inv_settings_file}")
        print(f"  {len(df_inv_settings)} records | {df_inv_settings[df_inv_settings['Is_Total']==True].shape[0]} totals")
    
    # 5. SALES ACTUAL
    df_sales_actual = pd.DataFrame(all_sales_actual)
    if not df_sales_actual.empty:
        df_sales_actual = df_sales_actual.sort_values(['Date', 'Site', 'Product'])
        sales_actual_file = os.path.join(output_dir, 'sales_actual.csv')
        df_sales_actual.to_csv(sales_actual_file, index=False)
        print(f"✓ SALES ACTUAL: {sales_actual_file}")
        print(f"  {len(df_sales_actual)} records | {df_sales_actual['Date'].min()} to {df_sales_actual['Date'].max()}")
    
    # Summary
    print(f"\n{'='*80}")
    print("✅ EXTRACTION COMPLETE!")
    print('='*80)
    print(f"Sites processed: {len(sites)}")
    print(f"Total readings: {len(df_readings):,}")
    print(f"Total loads: {len(df_loads):,}")
    print(f"Total tank sizes: {len(df_tank_sizes)}")
    print(f"Total inv settings: {len(df_inv_settings)}")
    print(f"Total sales actual: {len(df_sales_actual):,}")
    print(f"\nAll files saved to: {os.path.abspath(output_dir)}")
    print("\n🎯 Data is now in long format (tidy) and ready for analysis!")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Extraction cancelled by user")
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
