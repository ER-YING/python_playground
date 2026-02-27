import requests
import pandas as pd
from datetime import date, datetime
import json
import calendar
def get_stockiq_forecast_data():
    # API configuration
    url = "https://earthrated.stockiqtech.net/api/ItemSiteForecastDetail/"
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
    
    # NetSuite ABC filter
    abc_filter = ["A", "B", "C", "D", "I"]
    
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
        "ICode": "Item Code",
        "SCode": "Site Code", 
        "A": "Date",
        "B": "Forecast Qty",
        "ActualQuantitySold": "Actuals",
        "ForecastErrorUnits": "Forecast Error",
        "ForecastErrorPercent": "Forecast Error %",
        "ItemSiteCategory1Name": "Product Category",
        "ItemSiteCategory4Name": "NetSuite ABC",  # 🔄 CHANGED: Updated from "AbcClass" to "ItemSiteCategory4Name"
        "PrimarySupplierName": "#1 Supplier Name"
    }
    
    try:
        print("🚀 Making API call to StockIQ...")
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
            print(f"📊 Total records retrieved: {len(df)}")
            
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
            if "ICode" in df_filtered.columns:
                df_filtered = df_filtered[df_filtered["ICode"].astype(str).str.startswith('10')]
                print(f"  ✅ After Item Code filter (starts with '10'): {len(df_filtered):,} records")
            else:
                print("  ⚠️  Skipping Item Code filter - field not found")
            
            # Filter by NetSuite ABC
            # 🔄 CHANGED: Updated to use "ItemSiteCategory4Name" instead of "AbcClass"
            if "ItemSiteCategory4Name" in df_filtered.columns:
                # Show unique ABC values before filtering
                unique_abc = df_filtered["ItemSiteCategory4Name"].dropna().unique()
                print(f"  📊 Available ABC Classes: {sorted(list(unique_abc))}")
                df_filtered = df_filtered[df_filtered["ItemSiteCategory4Name"].isin(abc_filter)]
                print(f"  ✅ After NetSuite ABC filter: {len(df_filtered):,} records")
            else:
                print("  ⚠️  Skipping NetSuite ABC filter - field not found")
            
            # Filter by Product Category
            if "ItemSiteCategory1Name" in df_filtered.columns:
                # Show unique categories before filtering
                unique_cats = df_filtered["ItemSiteCategory1Name"].dropna().unique()
                print(f"  📊 Available Product Categories ({len(unique_cats)}): {sorted(list(unique_cats))}")
                df_filtered = df_filtered[df_filtered["ItemSiteCategory1Name"].isin(product_category_filter)]
                print(f"  ✅ After Product Category filter: {len(df_filtered):,} records")
            else:
                print("  ⚠️  Skipping Product Category filter - field not found")
            
            # Filter by Site Code
            if "SCode" in df_filtered.columns:
                # Show unique sites before filtering
                unique_sites = df_filtered["SCode"].dropna().unique()
                print(f"  📊 Available Site Codes: {sorted(list(unique_sites))}")
                df_filtered = df_filtered[df_filtered["SCode"].isin(site_code_filter)]
                print(f"  ✅ After Site Code filter: {len(df_filtered):,} records")
            else:
                print("  ⚠️  Skipping Site Code filter - field not found")
            
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
            # Format Forecast Error to 1 decimal place
            if "Forecast Error" in df_final.columns:
                df_final["Forecast Error"] = pd.to_numeric(df_final["Forecast Error"], errors='coerce').round(1)
                print(f"  📊 Formatted Forecast Error to 1 decimal place")
           # Format Forecast Error Percent to a percentage string with 1 decimal place
            if "Forecast Error %" in df_final.columns:
                s = pd.to_numeric(df_final["Forecast Error %"], errors='coerce')
                df_final["Forecast Error %"] = s.apply(lambda x: f'{x:.1%}' if pd.notnull(x) else '')
                print(f"  📊 Formatted 'Forecast Error %' to 1 decimal place")
            
            # Add new columns: Days in Month and Daily Forecast
            if "Date" in df_final.columns and "Forecast Qty" in df_final.columns:
                print(f"\n🆕 Adding new columns...")
                
                # Convert Date column to datetime if not already
                df_final["Date"] = pd.to_datetime(df_final["Date"], errors='coerce')
                
                # Calculate Days in Month
                def get_days_in_month(date_val):
                    if pd.isna(date_val):
                        return None
                    return calendar.monthrange(date_val.year, date_val.month)[1]
                
                df_final["Days in Month"] = df_final["Date"].apply(get_days_in_month)
                print(f"  ✅ Added 'Days in Month' column")
                
                # Calculate Daily Forecast (Forecast Qty / Days in Month) without decimals
                df_final["Daily Forecast"] = (df_final["Forecast Qty"] / df_final["Days in Month"]).round(0).astype('Int64')
                print(f"  ✅ Added 'Daily Forecast' column (no decimals)")
            
            # Sort data
            sort_columns = []
            if "Item Code" in df_final.columns:
                sort_columns.append("Item Code")
            if "Site Code" in df_final.columns:
                sort_columns.append("Site Code")
            if "Date" in df_final.columns:
                sort_columns.append("Date")
            
            if sort_columns:
                df_final = df_final.sort_values(sort_columns)
                print(f"  📋 Sorted by: {sort_columns}")
            
            # Create filename with today's date
            today = datetime.now().strftime("%Y-%m-%d")
            filename = f"Forecast Analysis- Item_Site_ Daily Forecast {today}.csv"
            
            # Save to CSV
            df_final.to_csv(filename, index=False)
            print(f"\n💾 Data successfully saved to '{filename}'")
            print(f"📊 Final dataset contains {len(df_final):,} records with {len(df_final.columns)} columns")
            
            # Display preview
            if not df_final.empty:
                print(f"\n👀 Preview of the final data:")
                print(df_final.head(5).to_string(max_cols=10, max_colwidth=30))
                
                # Summary statistics
                print(f"\n📈 Summary Statistics:")
                if "Item Code" in df_final.columns:
                    print(f"  📦 Unique Items: {df_final['Item Code'].nunique():,}")
                if "Site Code" in df_final.columns:
                    print(f"  🏢 Unique Sites: {df_final['Site Code'].nunique():,}")
                if "Date" in df_final.columns:
                    try:
                        date_range = f"{df_final['Date'].min()} to {df_final['Date'].max()}"
                        print(f"  📅 Date Range: {date_range}")
                    except:
                        print(f"  📅 Date Range: Unable to calculate")
                if "Forecast Qty" in df_final.columns:
                    try:
                        forecast_stats = df_final['Forecast Qty'].describe()
                        print(f"  📊 Forecast Qty - Min: {forecast_stats['min']:.2f}, Max: {forecast_stats['max']:.2f}, Mean: {forecast_stats['mean']:.2f}")
                    except:
                        print(f"  📊 Forecast Qty statistics: Unable to calculate")
                if "Daily Forecast" in df_final.columns:
                    try:
                        daily_forecast_stats = df_final['Daily Forecast'].describe()
                        print(f"  📊 Daily Forecast - Min: {daily_forecast_stats['min']:.0f}, Max: {daily_forecast_stats['max']:.0f}, Mean: {daily_forecast_stats['mean']:.0f}")
                    except:
                        print(f"  📊 Daily Forecast statistics: Unable to calculate")
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
                        
            else:
                print(f"\n❌ No data found matching the specified criteria.")
                print("Consider reviewing the filter criteria:")
                print("- Item codes starting with '10'")
                print("- NetSuite ABC classes: A, B, C, D, I")
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
    print("🚀 StockIQ Forecast Data Retrieval with Daily Forecast Analysis")
    print("=" * 70)
    print("Retrieving forecast data with the following filters:")
    print("✅ Item Code: Starting with '10'")
    print("✅ NetSuite ABC: A, B, C, D, I")
    print("✅ Product Categories: Cleanup, Grooming, Toys (specific types)")
    print("✅ Site Codes: 6 specific locations")
    print("✅ Date: From July 1, 2024 onwards")
    print("\nNew Features:")
    print("🆕 Days in Month: Calculates days in each month from Date column")
    print("🆕 Daily Forecast: Calculates Forecast Qty ÷ Days in Month (no decimals)")
    print("=" * 70)
    
    # Get the data
    result = get_stockiq_forecast_data()
    
    if result is not None and not result.empty:
        print(f"\n🎉 Process completed successfully!")
        print(f"📊 Retrieved and filtered {len(result):,} records")
        print(f"💾 Data saved to CSV file with today's date")
        print(f"📋 File includes new 'Days in Month' and 'Daily Forecast' columns")
    else:
        print(f"\n❌ Process completed but no data was retrieved or saved.")
        print(f"\n🔧 Troubleshooting suggestions:")
        print("1. Check if the filter criteria are too restrictive")
        print("2. Verify the authorization credentials are still valid") 
        print("3. Confirm the API parameters are correct")
        print("4. Review the available values shown in the filter output above")
if __name__ == "__main__":
    main()