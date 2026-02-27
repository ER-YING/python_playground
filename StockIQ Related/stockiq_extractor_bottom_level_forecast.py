import requests
import pandas as pd
from datetime import date, datetime
import json

def get_bottom_level_forecast_data():
    # API configuration
    url = "https://earthrated.stockiqtech.net/api/BottomLevelForecastDetail"
    headers = {
        'Authorization': 'Basic WWluZzpLeDUmZn1IM0Y4',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    params = {
        'interval': 4,
        'demandForecastSeriesId': 1
    }
    
    # Define filter criteria
    start_date = date(2024, 7, 1)
    
    # NetSuite ABC filter - updated to include N/A and Unassigned
    abc_filter = ["A", "B", "C", "D", "I", "N/A", "Unassigned"]
    
    # Product Category filter
    product_category_filter = [
        "Cleanup : Bags",
        "Cleanup : Dispensers", 
        "Grooming : Wipes",
        "Grooming : Specialty Wipes",
        "Grooming : Waterless",
        "Grooming : Shampoo",
        "Toys : Interactive Toys",
        "Toys : Standalone"
    ]
    
    # Site Code filter
    site_code_filter = [
        "Source NJ",
        "Northland Goreway",
        "Source Montebello",
        "Progressive UK",
        "Rhenus Netherlands",
        "Golden State FC LLC (AGL)"
    ]
    
    # Field mappings (StockIQ field name -> Desired field name)
    field_mappings = {
        "I": "Item",
        "S": "Site",
        "ISC1": "Product Category",
        "ISC4": "NetSuite ABC",
        "CSC2": "Customer",
        "A": "Date",
        "B": "Forecast Qty",
        "ActualQuantitySold": "Actuals",
        "ForecastErrorUnits": "Forecast Error",
        "ForecastErrorPercent": "Forecast Error %"
    }
    
    try:
        print("🚀 Making API call to StockIQ Bottom Level Forecast...")
        print(f"URL: {url}")
        print(f"Parameters: {params}")
        
        # Make API call
        response = requests.get(url, headers=headers, params=params, timeout=60)
        
        print(f"Response Status Code: {response.status_code}")
        
        # Check if request was successful
        if response.status_code == 200:
            print("✅ API call successful!")
            
            try:
                data = response.json()
            except json.JSONDecodeError as e:
                print(f"❌ Failed to parse JSON response: {e}")
                return None
            
            # Handle different response structures
            if isinstance(data, dict):
                # Look for data in common keys
                if 'results' in data:
                    records = data['results']
                elif 'data' in data:
                    records = data['data']
                else:
                    # Use the first array found or the data itself
                    for key, value in data.items():
                        if isinstance(value, list):
                            records = value
                            break
                    else:
                        records = [data]  # Single record
            elif isinstance(data, list):
                records = data
            else:
                print(f"❌ Unexpected data format: {type(data)}")
                return None
            
            if not records:
                print("❌ No records found in response")
                return None
            
            # Convert to DataFrame
            df = pd.DataFrame(records)
            print(f"📊 Total records retrieved: {len(df):,}")
            
            # Check if required fields exist
            missing_fields = []
            existing_fields = []
            
            for stockiq_field, display_name in field_mappings.items():
                if stockiq_field in df.columns:
                    existing_fields.append((stockiq_field, display_name))
                    print(f"✅ Found field: '{stockiq_field}' -> '{display_name}'")
                else:
                    missing_fields.append((stockiq_field, display_name))
                    print(f"❌ Missing field: '{stockiq_field}' -> '{display_name}'")
            
            if missing_fields:
                print(f"\n⚠️  Warning: {len(missing_fields)} field(s) missing from API response")
                print("Available columns:", list(df.columns))
            
            # Apply filters using actual StockIQ field names
            df_filtered = df.copy()
            initial_count = len(df_filtered)
            
            print(f"\n🔧 Applying filters...")
            print(f"Starting with {initial_count:,} records")
            
            # Filter by Item Code starting with "10"
            if "I" in df_filtered.columns:
                df_filtered = df_filtered[df_filtered["I"].astype(str).str.startswith('10')]
                print(f"  ✅ After Item filter (starts with '10'): {len(df_filtered):,} records")
            else:
                print("  ⚠️  Skipping Item filter - field not found")
            
            # Filter by NetSuite ABC
            if "ISC4" in df_filtered.columns:
                # Show unique ABC values before filtering
                unique_abc = df_filtered["ISC4"].dropna().unique()
                print(f"  📊 Available ABC Classes: {sorted(list(unique_abc))}")
                df_filtered = df_filtered[df_filtered["ISC4"].isin(abc_filter)]
                print(f"  ✅ After NetSuite ABC filter: {len(df_filtered):,} records")
            else:
                print("  ⚠️  Skipping NetSuite ABC filter - field not found")
            
            # Filter by Product Category
            if "ISC1" in df_filtered.columns:
                # Show unique categories before filtering
                unique_cats = df_filtered["ISC1"].dropna().unique()
                print(f"  📊 Available Product Categories ({len(unique_cats)}): {sorted(list(unique_cats))}")
                df_filtered = df_filtered[df_filtered["ISC1"].isin(product_category_filter)]
                print(f"  ✅ After Product Category filter: {len(df_filtered):,} records")
            else:
                print("  ⚠️  Skipping Product Category filter - field not found")
            
            # Filter by Site Code
            if "S" in df_filtered.columns:
                # Show unique sites before filtering
                unique_sites = df_filtered["S"].dropna().unique()
                print(f"  📊 Available Site Codes: {sorted(list(unique_sites))}")
                df_filtered = df_filtered[df_filtered["S"].isin(site_code_filter)]
                print(f"  ✅ After Site filter: {len(df_filtered):,} records")
            else:
                print("  ⚠️  Skipping Site filter - field not found")
            
            # Filter by date
            if "A" in df_filtered.columns:
                try:
                    df_filtered["A"] = pd.to_datetime(df_filtered["A"], errors='coerce')
                    date_filtered = df_filtered[df_filtered["A"] >= pd.to_datetime(start_date)]
                    print(f"  ✅ After date filter (>= {start_date}): {len(date_filtered):,} records")
                    df_filtered = date_filtered
                except Exception as e:
                    print(f"  ⚠️  Date filter failed: {e}")
            else:
                print("  ⚠️  Skipping date filter - field not found")
            
            if len(df_filtered) == 0:
                print("\n❌ No records remain after filtering. Consider relaxing filter criteria.")
                return pd.DataFrame()
            
            # Create final DataFrame with renamed columns
            df_final = pd.DataFrame()
            
            for stockiq_field, display_name in existing_fields:
                df_final[display_name] = df_filtered[stockiq_field]
            
            # Format Forecast Qty to 1 decimal place
            if "Forecast Qty" in df_final.columns:
                df_final["Forecast Qty"] = pd.to_numeric(df_final["Forecast Qty"], errors='coerce').round(1)
                print(f"  📊 Formatted Forecast Qty to 1 decimal place")
            
            # Sort data
            sort_columns = []
            if "Item" in df_final.columns:
                sort_columns.append("Item")
            if "Site" in df_final.columns:
                sort_columns.append("Site")
            if "Customer" in df_final.columns:
                sort_columns.append("Customer")
            if "Date" in df_final.columns:
                sort_columns.append("Date")
            
            if sort_columns:
                df_final = df_final.sort_values(sort_columns)
                print(f"  📋 Sorted by: {sort_columns}")
            
            # Create filename with today's date
            today = datetime.now().strftime("%Y-%m-%d")
            filename = f"Forecast Analysis - Bottom Level {today}.csv"
            
            # Save to CSV
            df_final.to_csv(filename, index=False)
            print(f"\n💾 Data successfully saved to '{filename}'")
            print(f"📊 Final dataset contains {len(df_final):,} records with {len(df_final.columns)} columns")
            
            # Display preview
            if not df_final.empty:
                print(f"\n👀 Preview of the final data:")
                print(df_final.head(5).to_string(max_cols=10, max_colwidth=25))
                
                # Summary statistics
                print(f"\n📈 Summary Statistics:")
                if "Item" in df_final.columns:
                    print(f"  📦 Unique Items: {df_final['Item'].nunique():,}")
                if "Site" in df_final.columns:
                    print(f"  🏢 Unique Sites: {df_final['Site'].nunique():,}")
                if "Customer" in df_final.columns:
                    print(f"  👥 Unique Customers: {df_final['Customer'].nunique():,}")
                if "Date" in df_final.columns:
                    try:
                        date_range = f"{df_final['Date'].min()} to {df_final['Date'].max()}"
                        print(f"  📅 Date Range: {date_range}")
                    except:
                        print(f"  📅 Date Range: Unable to calculate")
                if "Forecast Qty" in df_final.columns:
                    try:
                        forecast_stats = df_final['Forecast Qty'].describe()
                        print(f"  📊 Forecast Qty - Min: {forecast_stats['min']:.1f}, Max: {forecast_stats['max']:.1f}, Mean: {forecast_stats['mean']:.1f}")
                    except:
                        print(f"  📊 Forecast Qty statistics: Unable to calculate")
                if "Actuals" in df_final.columns:
                    try:
                        actuals_stats = df_final['Actuals'].describe()
                        print(f"  📊 Actuals - Min: {actuals_stats['min']:.2f}, Max: {actuals_stats['max']:.2f}, Mean: {actuals_stats['mean']:.2f}")
                    except:
                        print(f"  📊 Actuals statistics: Unable to calculate")
                        
                # Show breakdown by key categories
                if "Product Category" in df_final.columns:
                    cat_counts = df_final['Product Category'].value_counts()
                    print(f"\n📋 Records by Product Category:")
                    for cat, count in cat_counts.items():
                        print(f"  {cat}: {count:,} records")
                
                if "NetSuite ABC" in df_final.columns:
                    abc_counts = df_final['NetSuite ABC'].value_counts()
                    print(f"\n📋 Records by NetSuite ABC:")
                    for abc, count in abc_counts.items():
                        print(f"  {abc}: {count:,} records")
                
                if "Customer" in df_final.columns and df_final['Customer'].nunique() <= 20:
                    customer_counts = df_final['Customer'].value_counts().head(10)
                    print(f"\n📋 Top 10 Customers by Record Count:")
                    for customer, count in customer_counts.items():
                        print(f"  {customer}: {count:,} records")
                        
            else:
                print(f"\n❌ No data found matching the specified criteria.")
                print("Consider reviewing the filter criteria:")
                print("- Item codes starting with '10'")
                print("- NetSuite ABC classes: A, B, C, D, I, N/A, Unassigned")
                print("- Specific product categories and site codes")
                print("- Dates from July 1, 2024 onwards")
            
            return df_final
            
        else:
            print(f"❌ API call failed with status code: {response.status_code}")
            if response.status_code == 401:
                print("   🔒 Authorization failed. Please check your credentials.")
            elif response.status_code == 403:
                print("   🚫 Access forbidden. You may not have permission to access this endpoint.")
            elif response.status_code == 404:
                print("   🔍 Endpoint not found. Please verify the URL is correct.")
            
            print(f"Response content: {response.text[:500]}")
            return None
            
    except requests.exceptions.ConnectionError as e:
        print(f"❌ Connection Error: {e}")
        return None
    except requests.exceptions.Timeout as e:
        print(f"❌ Timeout Error: {e}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"❌ Request Error: {e}")
        return None
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return None

def main():
    print("🚀 StockIQ Bottom Level Forecast Data Retrieval")
    print("=" * 70)
    print("Retrieving bottom level forecast data with the following filters:")
    print("✅ Item: Starting with '10'")
    print("✅ NetSuite ABC: A, B, C, D, I, N/A, Unassigned")
    print("✅ Product Categories: Cleanup, Grooming, Toys (specific types)")
    print("✅ Site Codes: 6 specific locations")
    print("✅ Date: From July 1, 2024 onwards")
    print("✅ Forecast Qty: Rounded to 1 decimal place")
    print("=" * 70)
    
    # Get the data
    result = get_bottom_level_forecast_data()
    
    if result is not None and not result.empty:
        print(f"\n🎉 Process completed successfully!")
        print(f"📊 Retrieved and filtered {len(result):,} records")
        print(f"💾 Data saved to 'Forecast Analysis - Bottom Level {datetime.now().strftime('%Y-%m-%d')}.csv'")
    else:
        print(f"\n❌ Process completed but no data was retrieved or saved.")
        print(f"\n🔧 Troubleshooting suggestions:")
        print("1. Check if the filter criteria are too restrictive")
        print("2. Verify the authorization credentials are still valid") 
        print("3. Confirm the API parameters are correct")
        print("4. Review the available values shown in the filter output above")

if __name__ == "__main__":
    main()