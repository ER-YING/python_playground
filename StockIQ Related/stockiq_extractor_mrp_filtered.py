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
        print("Making API call to StockIQ...")
        response = requests.get(endpoint, headers=headers, params=params)
        
        if response.status_code == 200:
            data = response.json()
            print(f"Successfully retrieved {len(data)} records")
            
            df = pd.DataFrame(data)
            
            if not df.empty:
                print("\nAvailable fields in the report:")
                for i, field in enumerate(df.columns, 1):
                    print(f"{i:2d}. {field}")
                
                return df
            else:
                print("No data returned from the API")
                return None
                
        else:
            print(f"Error: API call failed with status code {response.status_code}")
            print(f"Response: {response.text}")
            return None
            
    except Exception as e:
        print(f"Error: {e}")
        return None

def apply_filters(df):
    """
    Apply filters to the dataframe - STRICT FILTERING
    """
    if df is None or df.empty:
        return df
    
    print(f"\n🔍 APPLYING STRICT FILTERS...")
    print(f"Original record count: {len(df)}")
    
    # Define filters - EXACTLY as specified
    target_sites = {
        "Source NJ", "Northland Goreway", "Source Montebello",
        "Progressive UK", "Rhenus Netherlands", "Golden State FC LLC (AGL)"
    }
    
    target_categories = {
        "Cleanup : Bags", "Cleanup : Dispensers", "Cleanup : Pee Pads", "Grooming : Wipes",
        "Grooming : Specialty Wipes", "Grooming : Waterless",
        "Grooming : Shampoo", "Toys : Interactive Toys", "Toys : Standalone"
    }
    
    # Item filter - items starting with "10"
    item_prefix = "10"
    
    # Show what's actually in the data BEFORE filtering
    print("\n" + "="*60)
    print("📋 CURRENT DATA ANALYSIS:")
    print("="*60)
    
    if 'SiteCode' in df.columns:
        unique_sites = sorted(df['SiteCode'].unique())
        print(f"🏢 ALL SITES IN DATA ({len(unique_sites)}):")
        for site in unique_sites:
            count = len(df[df['SiteCode'] == site])
            in_target = "✅" if site in target_sites else "❌"
            print(f"   {in_target} '{site}' ({count} records)")
    
    if 'ProductCategory' in df.columns:
        unique_categories = sorted(df['ProductCategory'].unique())
        print(f"\n📂 ALL CATEGORIES IN DATA ({len(unique_categories)}):")
        for category in unique_categories:
            count = len(df[df['ProductCategory'] == category])
            in_target = "✅" if category in target_categories else "❌"
            print(f"   {in_target} '{category}' ({count} records)")
    
    if 'ItemCode' in df.columns:
        # Show sample of item codes and count with prefix
        items_with_prefix = df[df['ItemCode'].astype(str).str.startswith(item_prefix)]
        items_without_prefix = df[~df['ItemCode'].astype(str).str.startswith(item_prefix)]
        print(f"\n🏷️  ITEM CODE ANALYSIS:")
        print(f"   ✅ Items starting with '{item_prefix}': {len(items_with_prefix)} records")
        print(f"   ❌ Items NOT starting with '{item_prefix}': {len(items_without_prefix)} records")
        
        # Show examples
        if len(items_with_prefix) > 0:
            print(f"   Examples with '{item_prefix}': {items_with_prefix['ItemCode'].head(5).tolist()}")
        if len(items_without_prefix) > 0:
            print(f"   Examples without '{item_prefix}': {items_without_prefix['ItemCode'].head(5).tolist()}")
    
    print("\n" + "="*60)
    print("🔄 APPLYING FILTERS:")
    print("="*60)
    
    # Start with original data
    filtered_df = df.copy()
    
    # Filter 1: Sites
    if 'SiteCode' in df.columns:
        before_count = len(filtered_df)
        filtered_df = filtered_df[filtered_df['SiteCode'].isin(target_sites)]
        after_count = len(filtered_df)
        print(f"🏢 Site filter: {before_count} → {after_count} records")
        
        if after_count == 0:
            print(f"   ⚠️  NO SITES MATCHED! Check if site names match exactly.")
        else:
            matched_sites = sorted(filtered_df['SiteCode'].unique())
            print(f"   ✅ Matched sites: {matched_sites}")
    
    # Filter 2: Categories (only if we still have data)
    if 'ProductCategory' in df.columns and len(filtered_df) > 0:
        before_count = len(filtered_df)
        filtered_df = filtered_df[filtered_df['ProductCategory'].isin(target_categories)]
        after_count = len(filtered_df)
        print(f"📂 Category filter: {before_count} → {after_count} records")
        
        if after_count == 0:
            print(f"   ⚠️  NO CATEGORIES MATCHED! Check if category names match exactly.")
        else:
            matched_categories = sorted(filtered_df['ProductCategory'].unique())
            print(f"   ✅ Matched categories: {matched_categories}")
    
    # Filter 3: Item prefix (only if we still have data)
    if 'ItemCode' in df.columns and len(filtered_df) > 0:
        before_count = len(filtered_df)
        filtered_df = filtered_df[filtered_df['ItemCode'].astype(str).str.startswith(item_prefix)]
        after_count = len(filtered_df)
        print(f"🏷️  Item prefix filter: {before_count} → {after_count} records")
        
        if after_count == 0:
            print(f"   ⚠️  NO ITEMS MATCHED prefix '{item_prefix}'!")
        else:
            print(f"   ✅ Items with prefix '{item_prefix}' retained")
    
    print("\n" + "="*60)
    print(f"📊 FINAL RESULT: {len(filtered_df)} records after all filters")
    print("="*60)
    
    if len(filtered_df) == 0:
        print("🚨 NO DATA REMAINS AFTER FILTERING!")
        print("💡 This means your filter criteria don't match the actual data values.")
        print("💡 Check the analysis above to see what values actually exist.")
        
        # Offer to continue with original data
        continue_choice = input("\n❓ Do you want to proceed with unfiltered data? (y/N): ").strip().lower()
        if continue_choice == 'y':
            print("📝 Proceeding with original data...")
            return df
    
    return filtered_df

def save_report_as_csv(df):
    """Save the report as CSV with today's date - ALWAYS INCLUDES ALL FIELDS"""
    if df is None or df.empty:
        print("❌ No data to save")
        return
    
    print(f"\n📊 Saving report with ALL FIELDS: {list(df.columns)}")
    
    # Generate filename
    today = datetime.now().strftime("%Y-%m-%d")
    filename = f"MRP_{today}.csv"
    
    # Save to CSV
    try:
        df.to_csv(filename, index=False)
        print(f"\n✅ Report saved successfully!")
        print(f"📁 Filename: {filename}")
        print(f"📍 Location: {os.path.abspath(filename)}")
        print(f"📊 Records: {len(df)}")
        print(f"🔧 Fields: {len(df.columns)} - {list(df.columns)}")
        
        # Show a preview of the data
        if len(df) > 0:
            print(f"\n👀 Preview (first 3 rows):")
            print(df.head(3).to_string(index=False))
        
    except Exception as e:
        print(f"❌ Error saving file: {e}")

def main():
    print("🚀 StockIQ Custom Report Generator")
    print("📋 Configuration: Always includes ALL FIELDS in output")
    print("🔍 Filters: Sites, Categories, and Item prefix as specified")
    print("-" * 60)
    
    # Get the report
    df = get_stockiq_custom_report()
    
    if df is not None:
        # Apply filters with detailed analysis
        filtered_df = apply_filters(df)
        
        if len(filtered_df) > 0:
            # Save the report with ALL fields (no field selection needed)
            save_report_as_csv(filtered_df)
        else:
            print("\n❌ No data to process after filtering.")
    else:
        print("Failed to retrieve report data")

if __name__ == "__main__":
    main()