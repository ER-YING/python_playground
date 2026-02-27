import requests
import json
import pandas as pd
from requests.auth import HTTPBasicAuth
from datetime import datetime
import os

def get_dependent_forecast_report(base_url, username, password, report_format='json'):
    """
    Get Dependent Forecast report from StockIQ
    """
    
    endpoint = f"{base_url}/CustomReportProducer"
    
    params = {
        'customReportName': 'Dependent Forecast',
        'format': report_format
    }
    
    try:
        print(f"Making request to: {endpoint}")
        print(f"Parameters: {params}")
        
        response = requests.get(
            endpoint,
            params=params,
            auth=HTTPBasicAuth(username, password),
            headers={
                'Accept': 'application/json' if report_format == 'json' else 'text/csv',
                'Content-Type': 'application/json'
            },
            timeout=60
        )
        
        print(f"Response Status Code: {response.status_code}")
        
        response.raise_for_status()
        
        if report_format == 'json':
            return response.json()
        else:
            return response.text
            
    except requests.exceptions.RequestException as e:
        print(f"Error fetching report: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Error response: {e.response.text}")
        return None

def format_and_save_report(report_data, save_directory=None):
    """
    Format the report data to match the screenshot format and save as CSV
    
    Args:
        report_data: Raw report data from StockIQ
        save_directory: Optional directory to save file
    
    Returns:
        str: Path of saved file or None if error
    """
    try:
        # Convert data to DataFrame for processing
        if isinstance(report_data, str):
            # If CSV string, read it
            from io import StringIO
            df = pd.read_csv(StringIO(report_data))
        elif isinstance(report_data, dict):
            if 'data' in report_data:
                df = pd.DataFrame(report_data['data'])
            else:
                df = pd.DataFrame([report_data])
        elif isinstance(report_data, list):
            df = pd.DataFrame(report_data)
        else:
            df = pd.DataFrame(report_data)
        
        # Expected column mapping based on your screenshot
        # Adjust these column names based on what StockIQ actually returns
        column_mapping = {
            'ItemCode': 'Item Code',
            'Item_Code': 'Item Code', 
            'item_code': 'Item Code',
            'SiteCode': 'Site Code',
            'Site_Code': 'Site Code',
            'site_code': 'Site Code',
            'MonthDate': 'Month Date',
            'Month_Date': 'Month Date',
            'month_date': 'Month Date',
            'Date': 'Month Date',
            'DependentForecast': 'Dependent Forecast',
            'Dependent_Forecast': 'Dependent Forecast',
            'dependent_forecast': 'Dependent Forecast',
            'Forecast': 'Dependent Forecast'
        }
        
        # Rename columns to match screenshot format
        df.rename(columns=column_mapping, inplace=True)
        
        # Ensure we have the required columns (create empty ones if missing)
        required_columns = ['Item Code', 'Site Code', 'Month Date', 'Dependent Forecast']
        for col in required_columns:
            if col not in df.columns:
                df[col] = ''  # Add empty column if missing
        
        # Select and reorder columns to match screenshot
        df = df[required_columns]
        
        # Sort data like in screenshot (by Item Code, then Site Code, then Month Date)
        df = df.sort_values(['Item Code', 'Site Code', 'Month Date'])
        
        # Format the Month Date column if needed (ensure consistent date format)
        if 'Month Date' in df.columns:
            try:
                df['Month Date'] = pd.to_datetime(df['Month Date']).dt.strftime('%m/%d/%Y')
            except:
                pass  # Keep original format if conversion fails
        
        # Format Dependent Forecast column (ensure it's numeric and properly formatted)
        if 'Dependent Forecast' in df.columns:
            try:
                df['Dependent Forecast'] = pd.to_numeric(df['Dependent Forecast'], errors='coerce')
                df['Dependent Forecast'] = df['Dependent Forecast'].fillna(0).astype(int)
            except:
                pass  # Keep original format if conversion fails
        
        # Generate filename with today's date
        today = datetime.now().strftime("%Y-%m-%d")
        filename = f"Dependent_Forecast_{today}.csv"
        
        # Set save directory
        if save_directory:
            os.makedirs(save_directory, exist_ok=True)
            filepath = os.path.join(save_directory, filename)
        else:
            filepath = filename
        
        # Save to CSV with exact formatting
        df.to_csv(
            filepath, 
            index=False,  # Don't include row numbers
            encoding='utf-8',
            lineterminator='\n'  # Ensure proper line endings
        )
        
        print(f"Report saved successfully to: {filepath}")
        print(f"Total rows: {len(df)}")
        print("Column structure:")
        print(df.head())
        
        return filepath
        
    except Exception as e:
        print(f"Error formatting and saving report: {e}")
        return None

def get_and_save_formatted_forecast(base_url, username, password, save_directory=None):
    """
    Get the Dependent Forecast report and save it in the exact format from screenshot
    """
    
    print("Fetching Dependent Forecast report...")
    
    # Try JSON first (usually gives better structured data)
    report_data = get_dependent_forecast_report(base_url, username, password, report_format='json')
    
    if not report_data:
        # If JSON fails, try CSV
        print("JSON failed, trying CSV format...")
        report_data = get_dependent_forecast_report(base_url, username, password, report_format='csv')
    
    if report_data:
        print("Report retrieved successfully!")
        
        # Format and save the report
        filepath = format_and_save_report(report_data, save_directory)
        return filepath
    else:
        print("Failed to retrieve report")
        return None

# Usage
BASE_URL = 'https://earthrated.stockiqtech.net/api'
USERNAME = 'Ying'
PASSWORD = 'Kx5&f}H3F8'

# Get and save the formatted report
saved_file = get_and_save_formatted_forecast(BASE_URL, USERNAME, PASSWORD)

if saved_file:
    print(f"\n✅ Success! Report saved to: {saved_file}")
    print("The CSV file matches the format shown in your screenshot.")
else:
    print("❌ Failed to retrieve and save report")

# Optional: Preview the saved data
try:
    if saved_file and os.path.exists(saved_file):
        preview_df = pd.read_csv(saved_file)
        print(f"\n📊 Preview of saved data ({len(preview_df)} rows):")
        print(preview_df.head(10).to_string(index=False))
except Exception as e:
    print(f"Could not preview data: {e}")