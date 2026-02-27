import requests
import csv
import json
from datetime import datetime, date

def fetch_stockiq_data():
    # API configuration
    url = "https://earthrated.stockiqtech.net/api/ItemSiteTurnsDetailOverTime/?interval=4"
    headers = {
        'Authorization': 'Basic WWluZzpLeDUmZn1IM0Y4',
        'Content-Type': 'application/json'
    }
    
    # Define field mappings: "Desired Name" -> "Actual API Field Name"
    field_mappings = {
        "Item": "ItemCode",
        "Site": "SiteCode", 
        "Period Date": "PeriodDate",
        "Qty Sold": "QuantitySold",
        "COGS": "Cogs",
        "On Hand": "OnHandQuantity",
        "Avg On Hand": "AverageOnHandQuantity",
        "Tgt On Hand": "TargetOnHandQuantity",
        "Unit Turns": "CurrentUnitTurns",
        "Avg Unit Turns": "AverageUnitTurns",
        "Tgt Unit Turns": "TargetUnitTurns",
        "Unit Avg Turns Vs Target": "AvgVsTargetUnitTurns",
        "Tgt On Hand $": "TargetOnHandBalance",
        "Avg On Hand $": "AverageOnHandBalance",
        "Product Category": "ItemSiteCategory1Name",
        "NetSuite ABC": "ItemSiteCategory4Name",
        "Avg Vs Target": "AvgVsTargetTurns",
        "Unit Turns Vs Target": "CurrVsTargetUnitTurns",
        "Measurement Period Days": "MeasurementPeriodLength"
    }
    
    # Define filter criteria
    target_sites = {
        "Source NJ", "Northland Goreway", "Source Montebello", 
        "Progressive UK", "Rhenus Netherlands"
    }
    
    target_abc = {"A", "B", "C", "D", "I"}
    
    target_categories = {
        "Cleanup : Bags", "Cleanup : Dispensers", "Cleanup : Pee Pads", "Grooming : Wipes", 
        "Grooming : Specialty Wipes", "Grooming : Waterless", 
        "Grooming : Shampoo", "Toys : Interactive Toys", "Toys : Standalone"
    }
    
    # Date filter - from 2024-07-01 onwards
    start_date = date(2024, 7, 1)
    
    # Item filter - items starting with "10"
    item_prefix = "10"
    
    try:
        # Make the API call
        print("Making API call to StockIQ...")
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            print("API call successful!")
            data = response.json()
            
            # Handle different possible data structures
            if isinstance(data, list):
                records = data
            elif isinstance(data, dict):
                records = data.get('data', data.get('results', data.get('items', [data])))
            else:
                records = [data]
            
            print(f"Processing {len(records)} records...")
            
            # Analyze date range and item codes in the data
            analyze_data(records)
            
            # Filter the data
            filtered_data = filter_data(records, field_mappings, target_sites, target_abc, 
                                     target_categories, start_date, item_prefix)
            
            print(f"\nFiltered data contains {len(filtered_data)} records")
            
            # Save to CSV
            if filtered_data:
                save_to_csv(filtered_data, list(field_mappings.keys()))
                print("Data successfully saved to 'Turns.csv'")
                
                # Show sample of filtered data
                print(f"\nSample of filtered data (first 3 records):")
                for i, record in enumerate(filtered_data[:3]):
                    print(f"Record {i+1}:")
                    for key, value in record.items():
                        if key in ["Item", "Site", "Period Date", "Product Category", "NetSuite ABC", "Qty Sold", "On Hand"]:
                            print(f"  {key}: {value}")
                    print()
                    
                # Show date range in filtered data
                dates = [record["Period Date"] for record in filtered_data if record["Period Date"]]
                if dates:
                    print(f"Date range in filtered data: {min(dates)} to {max(dates)}")
                    
                # Show sample of item codes in filtered data
                items = [record["Item"] for record in filtered_data[:10] if record["Item"]]
                if items:
                    print(f"Sample item codes in filtered data: {items}")
                    
            else:
                print("No data matches the filter criteria.")
                save_to_csv([], list(field_mappings.keys()))
                
        else:
            print(f"API call failed with status code: {response.status_code}")
            
    except Exception as e:
        print(f"An error occurred: {e}")

def analyze_data(records):
    """Analyze the date range and item codes in the data"""
    print("\nAnalyzing data...")
    
    # Analyze dates
    dates = []
    item_codes = []
    item_prefixes = {}
    
    for record in records[:1000]:  # Sample first 1000 records
        period_date = record.get("PeriodDate", "")
        item_code = record.get("ItemCode", "")
        
        if period_date:
            dates.append(period_date)
        if item_code:
            item_codes.append(item_code)
            # Count item prefixes (first 2 characters)
            prefix = str(item_code)[:2]
            item_prefixes[prefix] = item_prefixes.get(prefix, 0) + 1
    
    if dates:
        print(f"Sample period dates: {dates[:5]}")
        
        # Try to find min and max dates
        try:
            parsed_dates = []
            for d in dates:
                parsed_date = parse_date(d)
                if parsed_date:
                    parsed_dates.append(parsed_date)
            
            if parsed_dates:
                print(f"Date range in data: {min(parsed_dates)} to {max(parsed_dates)}")
        except Exception as e:
            print(f"Could not parse date range: {e}")
    
    if item_codes:
        print(f"Sample item codes: {item_codes[:10]}")
        print(f"Most common item prefixes:")
        sorted_prefixes = sorted(item_prefixes.items(), key=lambda x: x[1], reverse=True)
        for prefix, count in sorted_prefixes[:10]:
            marker = " ✓" if prefix == "10" else ""
            print(f"  '{prefix}': {count} items{marker}")

def parse_date(date_str):
    """Parse date string into date object"""
    if not date_str:
        return None
        
    # Handle different date formats
    date_formats = [
        "%Y/%m/%d",     # 2024/07/01
        "%Y-%m-%d",     # 2024-07-01
        "%m/%d/%Y",     # 07/01/2024
        "%d/%m/%Y",     # 01/07/2024
    ]
    
    date_str = str(date_str).strip()
    
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    
    # If none of the formats work, try to extract year, month, day
    try:
        if "/" in date_str:
            parts = date_str.split("/")
            if len(parts) == 3:
                # Assume YYYY/MM/DD if first part is 4 digits
                if len(parts[0]) == 4:
                    return date(int(parts[0]), int(parts[1]), int(parts[2]))
                # Otherwise assume MM/DD/YYYY
                else:
                    return date(int(parts[2]), int(parts[0]), int(parts[1]))
    except:
        pass
    
    return None

def filter_data(records, field_mappings, target_sites, target_abc, target_categories, start_date, item_prefix):
    """Filter data based on all criteria including date and item prefix"""
    filtered_data = []
    
    print(f"\nFILTERING DATA...")
    print(f"Target sites: {len(target_sites)} sites")
    print(f"Target categories: {len(target_categories)} categories")
    print(f"Target ABC values: {target_abc}")
    print(f"Start date filter: {start_date}")
    print(f"Item prefix filter: starts with '{item_prefix}'")
    
    site_matches = 0
    category_matches = 0
    abc_matches = 0
    date_matches = 0
    item_matches = 0
    full_matches = 0
    
    for record in records:
        site = str(record.get("SiteCode", "")).strip()
        category1 = str(record.get("ItemSiteCategory1Name", "")).strip()
        abc_value = str(record.get("ItemSiteCategory4Name", "")).strip()
        period_date = record.get("PeriodDate", "")
        item_code = str(record.get("ItemCode", "")).strip()
        
        # Check site match
        site_match = site in target_sites
        if site_match:
            site_matches += 1
        
        # Check category match
        category_match = category1 in target_categories
        if category_match:
            category_matches += 1
        
        # Check ABC match
        abc_match = abc_value in target_abc
        if abc_match:
            abc_matches += 1
        
        # Check date match
        date_match = True  # Default to True if we can't parse the date
        parsed_date = parse_date(period_date)
        if parsed_date:
            date_match = parsed_date >= start_date
            if date_match:
                date_matches += 1
        
        # Check item prefix match
        item_match = item_code.startswith(item_prefix)
        if item_match:
            item_matches += 1
        
        # If all criteria match, include the record
        if site_match and category_match and abc_match and date_match and item_match:
            full_matches += 1
            # Map the fields to your desired names
            filtered_record = {}
            for desired_name, api_field in field_mappings.items():
                filtered_record[desired_name] = record.get(api_field, "")
            
            filtered_data.append(filtered_record)
    
    print(f"Site matches: {site_matches}")
    print(f"Category matches: {category_matches}")
    print(f"ABC matches: {abc_matches}")
    print(f"Date matches (>= {start_date}): {date_matches}")
    print(f"Item matches (starts with '{item_prefix}'): {item_matches}")
    print(f"Records matching ALL criteria: {full_matches}")
    
    return filtered_data

def format_numeric_value(value):
    """Format numeric values to remove leading quotes and standardize format"""
    if value == "" or value is None:
        return ""
    
    # Convert to string and clean
    str_value = str(value).strip()
    
    # Remove leading quote if present
    if str_value.startswith("'"):
        str_value = str_value[1:]
    
    # Try to convert to number and format appropriately
    try:
        float_val = float(str_value)
        
        # Handle zero
        if float_val == 0:
            return "0"
        
        # Handle very small numbers (preserve more precision)
        elif abs(float_val) < 0.001:
            formatted = f"{float_val:.8f}".rstrip('0').rstrip('.')
            return formatted if formatted else "0"
        
        # Handle small numbers (4 decimal places)
        elif abs(float_val) < 1:
            return f"{float_val:.4f}".rstrip('0').rstrip('.')
        
        # Handle regular numbers (2 decimal places)
        elif abs(float_val) < 1000:
            return f"{float_val:.2f}".rstrip('0').rstrip('.')
        
        # Handle large numbers (round to whole numbers or 1 decimal)
        else:
            if float_val == int(float_val):
                return str(int(float_val))
            else:
                return f"{float_val:.1f}".rstrip('0').rstrip('.')
    
    except (ValueError, TypeError):
        # If it can't be converted to float, return the cleaned string
        return str_value

def save_to_csv(data, fieldnames):
    """Save the filtered data to a CSV file named 'Turns.csv' with clean number formatting"""
    filename = "Turns.csv"
    
    # Define which fields should be treated as numbers
    numeric_fields = {
        "Qty Sold", "COGS", "On Hand", "Avg On Hand", "Tgt On Hand", 
        "Unit Turns", "Avg Unit Turns", "Tgt Unit Turns", "Unit Avg Turns Vs Target",
        "Tgt On Hand $", "Avg On Hand $", "Avg Vs Target", "Unit Turns Vs Target", 
        "Measurement Period Days"
    }
    
    try:
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()
            
            for row in data:
                cleaned_row = {}
                for key, value in row.items():
                    if key in numeric_fields:
                        # Format numeric fields
                        cleaned_row[key] = format_numeric_value(value)
                    else:
                        # Clean text fields (remove leading quotes if present)
                        clean_value = str(value).strip()
                        if clean_value.startswith("'"):
                            clean_value = clean_value[1:]
                        cleaned_row[key] = clean_value
                
                writer.writerow(cleaned_row)
                
        print(f"CSV file '{filename}' has been created successfully with clean formatting!")
        print("✓ Fixed leading quote issues in numeric fields")
        print("✓ Standardized number formatting")
        
    except Exception as e:
        print(f"Error saving to CSV: {e}")

def main():
    print("Starting StockIQ data extraction with clean number formatting...")
    print("=" * 70)
    print(f"Execution time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    fetch_stockiq_data()
    
    print("=" * 70)
    print("Process completed!")

if __name__ == "__main__":
    main()
