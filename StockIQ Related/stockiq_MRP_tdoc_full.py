import requests
import pandas as pd
from datetime import datetime
import json
import os
import re
from typing import List

# --------- Step 1: API and Data Processing Functions ---------

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
    """Apply filters to the dataframe - STRICT FILTERING"""
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

def save_mrp_csv(df):
    """Save the MRP report as CSV with today's date"""
    if df is None or df.empty:
        print("❌ No data to save")
        return None
    
    print(f"\n📊 Saving MRP report with ALL FIELDS: {list(df.columns)}")
    
    # Generate filename
    today = datetime.now().strftime("%Y-%m-%d")
    filename = f"MRP_{today}.csv"
    
    # Save to CSV
    try:
        df.to_csv(filename, index=False)
        print(f"\n✅ MRP Report saved successfully!")
        print(f"📁 Filename: {filename}")
        print(f"📍 Location: {os.path.abspath(filename)}")
        print(f"📊 Records: {len(df)}")
        print(f"🔧 Fields: {len(df.columns)} - {list(df.columns)}")
        
        # Show a preview of the data
        if len(df) > 0:
            print(f"\n👀 Preview (first 3 rows):")
            print(df.head(3).to_string(index=False))
        
        return filename
        
    except Exception as e:
        print(f"❌ Error saving file: {e}")
        return None

# --------- Step 2: Target Days Processing Functions ---------

def normalize_key(x):
    """Normalize strings for matching keys."""
    if pd.isna(x):
        return ""
    s = str(x).replace("\u00A0", " ").strip()       # remove non-breaking spaces
    s = re.sub(r"\s+", " ", s)                      # collapse multiple spaces
    return s.upper()

def find_col(cols: List[str], preferred_exact: List[str], contains_any: List[str]):
    """Find column name in a DataFrame that matches preferred names."""
    lower = {c.lower(): c for c in cols}
    for name in preferred_exact:
        if name.lower() in lower:
            return lower[name.lower()]
    for c in cols:
        cl = c.lower()
        if any(pat.lower() in cl for pat in contains_any):
            return c
    raise KeyError(f"Could not find matching column in {cols}")

def process_with_target_days(mrp_filename):
    """Process MRP file with target days to create final report"""
    print(f"\n🔄 STEP 2: Processing {mrp_filename} with target days...")
    
    try:
        # Load data
        mrp_df = pd.read_csv(mrp_filename)
        
        # Check if target_days.xlsx exists
        if not os.path.exists("target_days.xlsx"):
            print("⚠️ target_days.xlsx not found. Please ensure the file is in the current directory.")
            return
        
        td = pd.read_excel("target_days.xlsx")
        
        print(f"📊 Loaded MRP data: {len(mrp_df)} records")
        print(f"📊 Loaded target days: {len(td)} records")
        
        # Identify columns in target_days
        item_col = find_col(td.columns, ["ItemCode", "Item Code", "Item"], ["item"])
        site_col = find_col(td.columns, ["SiteCode", "Site Code", "Site"], ["site", "warehouse", "location"])
        days_col = find_col(td.columns, ["# of Days", "Days", "Target Days"], ["days"])
        
        print(f"📋 Target days columns - Item: {item_col}, Site: {site_col}, Days: {days_col}")
        
        # Normalize keys in both files
        mrp_df["_ITEM_KEY"] = mrp_df["ItemCode"].map(normalize_key)
        mrp_df["_SITE_KEY"] = mrp_df["SiteCode"].map(normalize_key)
        td["_ITEM_KEY"] = td[item_col].map(normalize_key)
        td["_SITE_KEY"] = td[site_col].map(normalize_key)
        
        # Reduce target_days to unique keys
        td_reduced = (
            td[["_ITEM_KEY", "_SITE_KEY", days_col]]
            .dropna(subset=[days_col])
            .drop_duplicates(subset=["_ITEM_KEY", "_SITE_KEY"], keep="first")
        )
        
        # Create lookup dict
        target_map = td_reduced.set_index(["_ITEM_KEY", "_SITE_KEY"])[days_col].to_dict()
        
        # Month columns are everything after "Step"
        if "Step" not in mrp_df.columns:
            print("⚠️ 'Step' column not found in MRP data. Cannot identify month columns.")
            return
        
        month_columns = list(mrp_df.columns[mrp_df.columns.get_loc("Step") + 1:])
        print(f"📅 Found {len(month_columns)} month columns: {month_columns[:5]}...")  # Show first 5
        
        # Build final DataFrame
        final_parts = []
        fallback_used = []  # collect missing matches
        
        for (item_key, site_key), group in mrp_df.groupby(["_ITEM_KEY", "_SITE_KEY"], sort=False):
            final_parts.append(group)
            
            if (item_key, site_key) in target_map:
                days_value = target_map[(item_key, site_key)]
            else:
                days_value = 120
                fallback_used.append((group.iloc[0]["ItemCode"], group.iloc[0]["SiteCode"]))
            
            tdoc_row = {col: pd.NA for col in mrp_df.columns}
            head = group.iloc[0]
            tdoc_row["ProductCategory"] = head["ProductCategory"]
            tdoc_row["ItemCode"] = head["ItemCode"]
            tdoc_row["SiteCode"] = head["SiteCode"]
            tdoc_row["Step"] = "9 TDOC"
            for mc in month_columns:
                tdoc_row[mc] = days_value
            
            final_parts.append(pd.DataFrame([tdoc_row], columns=mrp_df.columns))
        
        final_df = pd.concat(final_parts, ignore_index=True)
        
        # Remove helper columns
        final_df = final_df.drop(columns=["_ITEM_KEY", "_SITE_KEY"])
        
        # Save file
        today = datetime.now().strftime("%Y-%m-%d")
        output_filename = f"MRP with Target Days_{today}.csv"
        final_df.to_csv(output_filename, index=False)
        
        # Reporting
        print(f"\n✅ Final file saved as '{output_filename}'")
        print(f"📊 Final records: {len(final_df)}")
        
        if fallback_used:
            print(f"⚠️ {len(fallback_used)} combinations used fallback 75 because no match was found in target_days.xlsx:")
            for item, site in fallback_used[:10]:  # Show first 10
                print(f"  - ItemCode: {item}, SiteCode: {site}")
            if len(fallback_used) > 10:
                print(f"  ... and {len(fallback_used) - 10} more")
        else:
            print("🎉 All combinations matched target_days.xlsx without fallback.")
            
    except Exception as e:
        print(f"❌ Error processing target days: {e}")

# --------- Main Function ---------

def main():
    print("🚀 COMBINED StockIQ MRP Report Generator with Target Days")
    print("="*70)
    print("STEP 1: Fetch data from StockIQ API and create MRP CSV")
    print("STEP 2: Process MRP CSV with target days to create final report")
    print("="*70)
    
    # STEP 1: Get the report from API
    print("\n" + "🔵" * 20 + " STEP 1 " + "🔵" * 20)
    df = get_stockiq_custom_report()
    
    if df is not None:
        # Apply filters with detailed analysis
        filtered_df = apply_filters(df)
        
        if len(filtered_df) > 0:
            # Save the MRP report
            mrp_filename = save_mrp_csv(filtered_df)
            
            if mrp_filename:
                # STEP 2: Process with target days
                print("\n" + "🟢" * 20 + " STEP 2 " + "🟢" * 20)
                process_with_target_days(mrp_filename)
                print("\n🎉 PROCESS COMPLETED! Check 'MRP with Target Days.csv' for the final result.")
            else:
                print("\n❌ Failed to save MRP file, cannot proceed to Step 2")
        else:
            print("\n❌ No data to process after filtering.")
    else:
        print("❌ Failed to retrieve report data from API")

if __name__ == "__main__":
    main()