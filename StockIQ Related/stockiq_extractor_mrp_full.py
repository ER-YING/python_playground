import requests
import pandas as pd
from datetime import datetime
import json
import os

def get_stockiq_custom_report():
    # Configuration
    BASE_URL = 'https://earthrated.stockiqtech.net/api/'
    CUSTOM_REPORT_ID = 4
    AUTHORIZATION = 'Basic WWluZzpLeDUmZn1IM0Y4'
    
    # API endpoint
    endpoint = f"{BASE_URL}CustomReportProducer"
    
    # Headers
    headers = {
        'Authorization': AUTHORIZATION,
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    # Parameters
    params = {
        'customReportId': CUSTOM_REPORT_ID
    }
    
    try:
        # Make API call
        print("Making API call to StockIQ...")
        response = requests.get(endpoint, headers=headers, params=params)
        
        # Check if request was successful
        if response.status_code == 200:
            # Parse JSON response
            data = response.json()
            print(f"Successfully retrieved {len(data)} records")
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            if not df.empty:
                # Display available fields
                print("\nAvailable fields in the report:")
                for i, field in enumerate(df.columns, 1):
                    print(f"{i}. {field}")
                
                return df
            else:
                print("No data returned from the API")
                return None
                
        else:
            print(f"Error: API call failed with status code {response.status_code}")
            print(f"Response: {response.text}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Error making API request: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response: {e}")
        return None

def filter_and_save_report(df, selected_fields=None):
    """
    Filter the report data and save as CSV
    
    Args:
        df: DataFrame containing the report data
        selected_fields: List of field names to include (if None, includes all fields)
    """
    if df is None or df.empty:
        print("No data to save")
        return
    
    # Filter fields if specified
    if selected_fields:
        # Validate that selected fields exist
        available_fields = df.columns.tolist()
        valid_fields = [field for field in selected_fields if field in available_fields]
        
        if valid_fields:
            df_filtered = df[valid_fields]
            print(f"Filtered to include fields: {valid_fields}")
        else:
            print("No valid fields found in selection, using all fields")
            df_filtered = df
    else:
        df_filtered = df
    
    # Generate filename with today's date
    today = datetime.now().strftime("%Y-%m-%d")
    filename = f"MRP_{today}.csv"
    
    # Save to CSV
    try:
        df_filtered.to_csv(filename, index=False)
        print(f"\nReport saved successfully as: {filename}")
        print(f"File location: {os.path.abspath(filename)}")
        print(f"Records saved: {len(df_filtered)}")
        print(f"Fields included: {len(df_filtered.columns)}")
        
    except Exception as e:
        print(f"Error saving CSV file: {e}")

def main():
    # Get the report data
    df = get_stockiq_custom_report()
    
    if df is not None:
        # Example: You can specify which fields to include
        # Leave as None to include all fields, or specify a list like:
        # selected_fields = ['ProductCode', 'Description', 'QtyOnHand', 'Price']
        selected_fields = None
        
        # Uncomment and modify the following line to filter specific fields:
        # selected_fields = ['field1', 'field2', 'field3']  # Replace with actual field names
        
        # Save the filtered report
        filter_and_save_report(df, selected_fields)

if __name__ == "__main__":
    main()