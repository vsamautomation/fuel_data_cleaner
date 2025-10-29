@echo off
REM Fuel Data Extractor - Windows Launcher
REM Double-click this file to run the extractor

title Fuel Data Extractor

echo ================================================================================
echo                        FUEL DATA EXTRACTOR
echo ================================================================================
echo.
echo Checking Python installation...

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python is not installed!
    echo.
    echo Please install Python from: https://www.python.org/downloads/
    echo Make sure to check "Add Python to PATH" during installation.
    echo.
    pause
    exit /b 1
)

echo Python found!
echo.
echo Installing required packages...
python -m pip install --quiet pandas requests

echo.
echo Starting extraction...
echo Output location: %USERPROFILE%\Desktop\fuel_data
echo.
echo This may take 30-60 seconds. Please wait...
echo.

REM Run the extractor
python extract_fuel_data.py -o "%USERPROFILE%\Desktop\fuel_data"

if errorlevel 1 (
    echo.
    echo ================================================================================
    echo ERROR: Extraction failed!
    echo Please check your internet connection and try again.
    echo ================================================================================
) else (
    echo.
    echo ================================================================================
    echo SUCCESS! Data extracted successfully.
    echo.
    echo Files saved to: %USERPROFILE%\Desktop\fuel_data
    echo.
    echo You can now open these CSV files in Excel.
    echo ================================================================================
)

echo.
pause
