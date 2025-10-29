#!/bin/bash
#
# Fuel Data Extractor - macOS Launcher
# Double-click this file to run the extractor
#

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Set output directory to user's Desktop
OUTPUT_DIR="$HOME/Desktop/fuel_data"

# Clear the terminal
clear

echo "================================================================================"
echo "                        FUEL DATA EXTRACTOR"
echo "================================================================================"
echo ""
echo "Checking Python installation..."

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "ERROR: Python 3 is not installed!"
    echo ""
    echo "Please install Python from: https://www.python.org/downloads/"
    echo ""
    echo "Press any key to exit..."
    read -n 1 -s
    exit 1
fi

echo "Python found!"
echo ""
echo "Installing required packages..."
python3 -m pip install --quiet pandas requests

echo ""
echo "Starting extraction..."
echo "Output location: $OUTPUT_DIR"
echo ""
echo "This may take 30-60 seconds. Please wait..."
echo ""

# Run the extractor
python3 extract_fuel_data.py -o "$OUTPUT_DIR"

# Capture the exit code
EXIT_CODE=$?

echo ""
echo "================================================================================"

if [ $EXIT_CODE -eq 0 ]; then
    echo "✓ SUCCESS! Data extracted successfully."
    echo ""
    echo "Files saved to:"
    echo "  $OUTPUT_DIR"
    echo ""
    echo "You can now open these CSV files in Excel."
else
    echo "✗ ERROR: Extraction failed with code $EXIT_CODE"
    echo ""
    echo "Please check your internet connection and try again."
fi

echo "================================================================================"
echo ""
echo "Press any key to exit..."
read -n 1 -s
