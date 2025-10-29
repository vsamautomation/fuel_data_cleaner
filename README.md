# Fuel Data Extractor

Extracts fuel inventory data from Google Sheets and converts it into clean CSV files ready for Excel analysis.

## What it does

This tool automatically:
- Fetches data from your published Google Sheets in real-time
- Identifies all 26+ fuel sites dynamically (no manual configuration needed)
- Extracts 672 dates of historical data (March 2024 - January 2026)
- Outputs 4 CSV files ready for Excel forecasting:
  - `fuel_readings.csv` - Daily tank readings (~45,000+ records)
  - `tank_sizes.csv` - Tank capacities (~95 records)
  - `inv_settings.csv` - Desired inventory levels (~99 records)
  - `sales_actual.csv` - Actual sales data (~35,000+ records)

## Quick Start

### For Windows Users (Double-click method)

1. **Download this repository**
   - Click the green "Code" button → "Download ZIP"
   - Extract the ZIP file

2. **Install Python (if not already installed)**
   - Download from: https://www.python.org/downloads/
   - ⚠️ **IMPORTANT**: Check "Add Python to PATH" during installation

3. **Run the extractor**
   - Double-click `RUN_EXTRACTOR.bat`
   - Files will be saved to: `C:\Users\YourName\Desktop\fuel_data\`

### For macOS Users (Double-click method)

1. **Download this repository**
   - Click the green "Code" button → "Download ZIP"
   - Extract the ZIP file

2. **Run the extractor**
   - Double-click `RUN_EXTRACTOR.command`
   - If prompted, click "Open" to allow execution
   - Files will be saved to: `~/Desktop/fuel_data/`

### Advanced Usage (Terminal/Command Line)

**Windows:**
```cmd
python extract_fuel_data.py -o C:\path\to\output
```

**macOS/Linux:**
```bash
python3 extract_fuel_data.py -o /path/to/output
```

## Files in this Repository

- `extract_fuel_data.py` - Main extraction script
- `site_identifier.py` - Dynamic site discovery module
- `RUN_EXTRACTOR.bat` - Windows launcher (double-click)
- `RUN_EXTRACTOR.command` - macOS launcher (double-click)
- `README.md` - This file

## Output Files

### fuel_readings.csv
Daily tank readings for all sites and products (87, 91, diesel).

| Date | Site | Product | Tank_Number | Reading | Is_Total |
|------|------|---------|-------------|---------|----------|
| 2024-03-01 | OLD Morongo | 87 | 1 | 17312 | False |
| 2024-03-01 | OLD Morongo | 87 | 2 | 16932 | False |

### tank_sizes.csv
Tank capacity information for all sites.

| Site | Product | Tank_Number | Tank_Size | Is_Total |
|------|---------|-------------|-----------|----------|
| OLD Morongo | 87 | 1 | 20000 | False |
| OLD Morongo | 87 | 0 | 40000 | True |

### inv_settings.csv
Desired inventory levels for efficient operations.

| Site | Product | Tank_Number | Desired_Level | Is_Total |
|------|---------|-------------|---------------|----------|
| OLD Morongo | 87 | 1 | 15000 | False |
| OLD Morongo | 87 | 0 | 30000 | True |

### sales_actual.csv
Actual daily sales for all sites and products.

| Date | Site | Product | Sales_Actual | Is_Total |
|------|------|---------|--------------|----------|
| 2024-03-01 | OLD Morongo | 87 | 32348 | False |
| 2024-03-01 | OLD Morongo | 91 | 5808 | False |

## Building Windows Executable (Optional)

If you want to create a standalone `.exe` file for Windows:

1. Install PyInstaller:
   ```cmd
   pip install pyinstaller
   ```

2. Build the executable:
   ```cmd
   pyinstaller --onefile --name FuelDataExtractor extract_fuel_data.py
   ```

3. The executable will be in the `dist/` folder

## Requirements

- Python 3.7+
- pandas
- requests

These are automatically installed when using the launcher scripts.

## Troubleshooting

### Windows: "Python is not recognized"
- Reinstall Python and check "Add Python to PATH"
- Or manually add Python to PATH in System Environment Variables

### macOS: "Cannot be opened because it is from an unidentified developer"
- Right-click `RUN_EXTRACTOR.command` → "Open" → "Open" (do this once)

### "No sites found" error
- Check your internet connection
- Verify the Google Sheets URL is accessible

### Empty output files
- Some sites may have incomplete data in the source sheet
- Check the console output for warnings about specific sites

## Support

For issues or questions, contact your system administrator.

## License

Internal use only.
