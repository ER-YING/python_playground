import requests
import pandas as pd
import json
from datetime import datetime

def explore_stockiq_api():
    """
    Function to call StockIQ API and explore the data structure with multiple complete records
    """
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
    
    try:
        print("🔍 Exploring StockIQ API Structure")
        print("=" * 50)
        print(f"URL: {url}")
        print(f"Parameters: {params}")
        print("\nMaking API call...")
        
        # Make API call
        response = requests.get(url, headers=headers, params=params, timeout=30)
        
        print(f"✅ Response Status Code: {response.status_code}")
        
        if response.status_code == 200:
            print("🎉 API call successful!\n")
            
            # Parse JSON response
            try:
                data = response.json()
                print(f"📊 Successfully parsed JSON response")
            except json.JSONDecodeError as e:
                print(f"❌ Failed to parse JSON: {e}")
                print("Raw response preview:", response.text[:500])
                return None
            
            # Analyze response structure
            print(f"\n📋 RESPONSE STRUCTURE:")
            print(f"Response type: {type(data)}")
            
            if isinstance(data, dict):
                print(f"Top-level keys: {list(data.keys())}")
                
                # Find the records array
                records = None
                for key, value in data.items():
                    if isinstance(value, list) and value:
                        print(f"  📦 '{key}': array with {len(value)} items")
                        if isinstance(value[0], dict):
                            print(f"      Sample keys: {list(value[0].keys())[:10]}")
                        if records is None:  # Use first array found
                            records = value
                
                if records is None:
                    print("❌ No data arrays found in response")
                    return None
                    
            elif isinstance(data, list):
                records = data
                print(f"Direct array with {len(records)} items")
            else:
                print(f"❌ Unexpected response format: {type(data)}")
                return None
            
            # Convert to DataFrame
            df = pd.DataFrame(records)
            print(f"\n📊 DATASET INFO:")
            print(f"   Total Rows: {len(df)}")
            print(f"   Total Columns: {len(df.columns)}")
            
            # List ALL columns with sample data and statistics
            print(f"\n📋 ALL AVAILABLE COLUMNS:")
            print("=" * 100)
            
            for i, col in enumerate(df.columns, 1):
                # Get sample values (non-null)
                non_null_values = df[col].dropna()
                sample_values = non_null_values.head(3).tolist()
                null_count = df[col].isnull().sum()
                unique_count = df[col].nunique()
                
                print(f"{i:2d}. '{col}'")
                print(f"    📊 Non-null: {len(non_null_values)}/{len(df)} | Unique: {unique_count}")
                print(f"    🔍 Sample values: {sample_values}")
                
                # Show data type
                dtype = df[col].dtype
                if dtype == 'object':
                    print(f"    📝 Type: Text/String")
                elif dtype in ['int64', 'float64']:
                    print(f"    🔢 Type: Number ({dtype})")
                else:
                    print(f"    📋 Type: {dtype}")
                print()
            
            # Show first 10 COMPLETE records (records with the most non-null values)
            print(f"🔍 FIRST 10 MOST COMPLETE RECORDS:")
            print("=" * 100)
            
            # Calculate completeness score for each row (number of non-null values)
            df['_completeness_score'] = df.count(axis=1)
            
            # Sort by completeness (most complete first) and take first 10
            most_complete_df = df.nlargest(10, '_completeness_score')
            
            for idx, (row_idx, row) in enumerate(most_complete_df.iterrows(), 1):
                completeness = row['_completeness_score']
                total_cols = len(df.columns) - 1  # Subtract the completeness score column
                
                print(f"\n--- RECORD {idx} (Row {row_idx}) - {completeness}/{total_cols} fields populated ---")
                
                for col in df.columns:
                    if col == '_completeness_score':
                        continue
                    value = row[col]
                    
                    # Format the output based on value type
                    if pd.isna(value):
                        print(f"  {col:25}: ❌ NULL/NaN")
                    elif isinstance(value, str) and value.strip() == '':
                        print(f"  {col:25}: ❌ Empty string")
                    elif isinstance(value, (int, float)) and value == 0:
                        print(f"  {col:25}: 🔢 {value} (zero)")
                    else:
                        # Truncate very long values
                        str_value = str(value)
                        if len(str_value) > 50:
                            str_value = str_value[:47] + "..."
                        print(f"  {col:25}: ✅ {str_value}")
            
            # Remove the helper column
            df = df.drop('_completeness_score', axis=1)
            
            # Look for potential "Forecast Qty" columns specifically
            print(f"\n🎯 SEARCHING FOR FORECAST/QUANTITY RELATED COLUMNS:")
            print("=" * 80)
            
            forecast_keywords = ['forecast', 'qty', 'quantity', 'demand', 'plan', 'prediction']
            potential_forecast_cols = []
            
            for col in df.columns:
                col_lower = col.lower()
                if any(keyword in col_lower for keyword in forecast_keywords):
                    potential_forecast_cols.append(col)
            
            if potential_forecast_cols:
                print("🔍 Potential Forecast/Quantity columns found:")
                for col in potential_forecast_cols:
                    non_zero_values = df[col][df[col] != 0].dropna()
                    sample_values = non_zero_values.head(5).tolist()
                    print(f"  📊 '{col}':")
                    print(f"      Non-zero values: {len(non_zero_values)}")
                    print(f"      Sample non-zero values: {sample_values}")
                    print()
            else:
                print("❌ No obvious forecast/quantity columns found by name")
                print("🔍 Looking for numeric columns that might contain forecast data:")
                
                numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
                for col in numeric_cols:
                    non_zero_values = df[col][df[col] != 0].dropna()
                    if len(non_zero_values) > 0:
                        sample_values = non_zero_values.head(3).tolist()
                        print(f"  🔢 '{col}': {len(non_zero_values)} non-zero values, samples: {sample_values}")
            
            # Save raw data
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"stockiq_exploration_{timestamp}.csv"
            df.to_csv(filename, index=False)
            print(f"\n💾 Raw data saved to: {filename}")
            
            print(f"\n" + "=" * 100)
            print("✨ ANALYSIS COMPLETE!")
            print("🎯 Look for your target fields in the complete records above:")
            print("   - Item Code (might be 'SKU', 'Item', 'ProductCode', etc.)")
            print("   - Site Code (might be 'Location', 'Warehouse', 'Site', etc.)")
            print("   - Date (might be 'Period', 'ForecastDate', etc.)")
            print("   - Forecast Qty (check the potential forecast columns identified)")
            print("   - Actuals (might be 'ActualQty', 'Historical', etc.)")
            print("   - Product Category (might be 'Category', 'ProductType', etc.)")
            print("   - NetSuite ABC (might be 'ABCClass', 'Classification', etc.)")
            print("   - #1 Supplier Name (might be 'Supplier', 'Vendor', etc.)")
            print("=" * 100)
            
            return df
            
        elif response.status_code == 401:
            print("❌ 401 Unauthorized - Check your credentials")
        elif response.status_code == 403:
            print("❌ 403 Forbidden - Access denied")
        elif response.status_code == 404:
            print("❌ 404 Not Found - Check the URL")
        else:
            print(f"❌ HTTP {response.status_code}")
            print(f"Response: {response.text[:300]}")
        
        return None
        
    except requests.exceptions.ConnectionError as e:
        print(f"❌ Connection Error: {e}")
        print("\n🔧 Troubleshooting:")
        print("1. Check if the URL is correct: https://earthrated.stockiqtech.net")
        print("2. Verify your internet connection")
        print("3. The server might be down or blocking requests")
        return None
        
    except requests.exceptions.Timeout as e:
        print(f"❌ Timeout Error: {e}")
        print("The request took too long - try again")
        return None
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return None

if __name__ == "__main__":
    print("🚀 StockIQ API Data Explorer - Enhanced Version")
    print("This will show you the 10 most complete records to help identify fields")
    print()
    
    result = explore_stockiq_api()
    
    if result is not None:
        print(f"\n✅ Exploration completed successfully!")
        print("📝 Use the complete records above to identify the correct field mappings")
    else:
        print(f"\n❌ Could not connect to StockIQ API")