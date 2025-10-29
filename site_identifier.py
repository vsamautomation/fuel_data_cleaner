"""
Site Identification Module
Dynamically identifies all sites by finding INV. SETTING labels
and getting the site name from the row directly above
"""

import pandas as pd
import re


def identify_sites(df, max_rows_to_scan=None):
    """
    Dynamically identify all sites in the fuel data
    
    Strategy: Find all rows with "INV. SETTING" in column 1,
    then get the site name from the row directly above
    
    Args:
        df: pandas DataFrame with Google Sheets data
        max_rows_to_scan: Maximum number of rows to scan (default: None = all rows)
    
    Returns:
        List of tuples: [(row_index, site_name), ...]
    """
    print("Identifying sites dynamically...")
    print("Strategy: Finding 'INV. SETTING' labels and extracting site names\n")
    
    sites = []
    rows_to_scan = len(df) if max_rows_to_scan is None else min(max_rows_to_scan, len(df))
    
    for idx in range(1, rows_to_scan):  # Start at 1 to ensure we can look back
        # Check column 1 for "INV. SETTING"
        cell_value = str(df.iloc[idx, 1]).strip() if pd.notna(df.iloc[idx, 1]) else ""
        
        if not cell_value:
            continue
        
        # Check if this cell contains "INV. SETTING" or "INV SETTING"
        if "INV. SETTING" in cell_value.upper() or "INV SETTING" in cell_value.upper():
            # Get the site name from the row directly above
            site_row = idx - 1
            site_name_raw = str(df.iloc[site_row, 1]).strip() if pd.notna(df.iloc[site_row, 1]) else ""
            
            if site_name_raw:
                # Clean up the site name
                site_name = clean_site_name(site_name_raw)
                
                # Avoid duplicates
                if not any(site[1] == site_name for site in sites):
                    sites.append((site_row, site_name))
                    print(f"  ✓ Row {site_row}: {site_name}")
    
    if not sites:
        print("  ⚠️  No sites found!")
    else:
        print(f"\n✓ Total sites identified: {len(sites)}")
    
    return sites


def clean_site_name(raw_name):
    """
    Clean up site name by removing extra characters and formatting
    
    Args:
        raw_name: Raw site name from the sheet
    
    Returns:
        str: Cleaned site name
    """
    # Remove common prefixes that aren't part of the name
    name = raw_name.strip()
    
    # Remove leading numbers and single letters (e.g., "1a OLD Morongo" -> "OLD Morongo")
    name = re.sub(r'^[\d\s]+[a-z]?\s+', '', name, flags=re.IGNORECASE)
    
    # Remove trailing special characters
    name = re.sub(r'[\s\|\-]+$', '', name)
    
    # Clean up extra whitespace
    name = ' '.join(name.split())
    
    return name


def get_site_info(df, site_row):
    """
    Get additional information about a site to validate it has expected structure
    
    Args:
        df: pandas DataFrame
        site_row: Row index of site
    
    Returns:
        dict: Site information including validation flags
    """
    info = {
        'row': site_row,
        'name': clean_site_name(str(df.iloc[site_row, 1]).strip()) if pd.notna(df.iloc[site_row, 1]) else "Unknown",
        'has_readings': False,
        'has_tank_sizes': False,
        'has_inv_settings': False
    }
    
    # Check for data sections within next 40 rows
    for offset in range(40):
        row_idx = site_row + offset
        if row_idx >= len(df):
            break
        
        # Check both column 1 and column 3 for section labels
        for col in [1, 3]:
            cell = str(df.iloc[row_idx, col]).strip().upper() if pd.notna(df.iloc[row_idx, col]) else ""
            
            if 'READINGS' in cell and 'AM READING' not in cell:
                info['has_readings'] = True
            if 'TANK SIZE' in cell:
                info['has_tank_sizes'] = True
            if 'INV. SETTING' in cell or 'INV SETTING' in cell:
                info['has_inv_settings'] = True
    
    return info


def validate_sites(df, sites):
    """
    Validate identified sites and return detailed report
    
    Args:
        df: pandas DataFrame
        sites: List of (row_index, site_name) tuples
    
    Returns:
        List of site info dictionaries
    """
    print("\n" + "="*80)
    print("VALIDATING IDENTIFIED SITES")
    print("="*80)
    
    sites_info = []
    
    for site_row, site_name in sites:
        info = get_site_info(df, site_row)
        sites_info.append(info)
    
    return sites_info


def print_site_report(sites_info):
    """
    Print a detailed report of identified sites
    
    Args:
        sites_info: List of site info dictionaries
    """
    print(f"\n{'Row':<6} {'Site Name':<40} {'Readings':<10} {'Tanks':<10} {'Inv Set'}")
    print("-"*80)
    
    for info in sites_info:
        readings = "✓" if info['has_readings'] else "✗"
        tanks = "✓" if info['has_tank_sizes'] else "✗"
        inv_settings = "✓" if info['has_inv_settings'] else "✗"
        
        print(f"{info['row']:<6} {info['name']:<40} {readings:<10} {tanks:<10} {inv_settings}")
    
    print("="*80)
    
    # Summary
    complete_sites = sum(1 for info in sites_info if info['has_readings'] and info['has_tank_sizes'] and info['has_inv_settings'])
    print(f"\n✓ Sites with complete data: {complete_sites}/{len(sites_info)}")


if __name__ == "__main__":
    # Test the module
    import requests
    from io import StringIO
    
    GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRpva-TXUaQR_6tJoXX2vnSN2ertC5GNxAgssqmXvIhqHBNrscDxSxtiSWbCiiHqAoSHb3SzXDQw_VX/pub?gid=1048590026&single=true&output=csv"
    
    print("SITE IDENTIFICATION MODULE - TEST")
    print("="*80)
    print("Fetching test data...")
    response = requests.get(GOOGLE_SHEET_URL, timeout=30)
    df = pd.read_csv(StringIO(response.text), header=None, low_memory=False)
    print(f"✓ Data shape: {df.shape}\n")
    
    # Identify sites
    sites = identify_sites(df)
    
    # Validate and get detailed info
    sites_info = validate_sites(df, sites)
    
    # Print report
    print_site_report(sites_info)
    
    print("\n✅ Site identification complete!")
