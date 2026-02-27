import requests
import pandas as pd
from datetime import datetime
import json
import os
import re
from typing import List
import warnings

# Suppress the specific FutureWarning for concat
warnings.filterwarnings('ignore', category=FutureWarning, message='.*DataFrame concatenation.*')

# --------- Step 1: API and Data Processing Functions (keeping same as before) ---------

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

def get_excel_column_name(col_index):
    """Convert 0-based column index to Excel column name (0->A, 1->B, etc.)"""
    result = ""
    while col_index >= 0:
        result = chr(col_index % 26 + ord('A')) + result
        col_index = col_index // 26 - 1
        if col_index < 0:
            break
    return result

def create_enhanced_rows(group, month_columns, days_value, starting_row_num):
    """Create enhanced rows with Simulated Demand, Simulated Order, and correct formulas"""
    
    # Get the basic info from the first row
    head = group.iloc[0]
    
    # Create a dictionary to store step data
    step_data = {}
    
    # Process existing rows
    for _, row in group.iterrows():
        step_data[row['Step']] = row
    
    # Define the correct order with new steps and their row positions
    ordered_steps = [
        ("1 Beginning On Hand", 0),      # Row offset 0
        ("2 Demand", 1),                 # Row offset 1  
        ("Simulated demand", 2),         # Row offset 2 (NEW)
        ("3 Forecast", 3),               # Row offset 3
        ("4 PO On Order", 4),            # Row offset 4
        ("5 Shipment", 5),               # Row offset 5
        ("6 Suggested PO Receipt", 6),   # Row offset 6
        ("Simulated Order", 7),          # Row offset 7 (NEW)
        ("7 Ending Inventory", 8),       # Row offset 8
        ("8 DOC - End of Month", 9),     # Row offset 9
        ("9 TDOC", 10)                   # Row offset 10
    ]
    
    # Create ordered rows list
    ordered_rows = []
    
    # Find the column index where month data starts (after "Step")
    step_col_index = None
    for i, col in enumerate(group.columns):
        if col == 'Step':
            step_col_index = i
            break
    
    first_month_col_index = step_col_index + 1 if step_col_index is not None else 4
    
    for step, row_offset in ordered_steps:
        current_row_num = starting_row_num + row_offset
        
        if step in step_data:
            # Use existing data
            row_dict = step_data[step].to_dict()
            ordered_rows.append(row_dict)
        elif step == "Simulated demand":
            # Create new Simulated demand row
            sim_demand_row = step_data["2 Demand"].to_dict().copy()
            sim_demand_row['Step'] = "Simulated demand"
            # Set all month values to 0
            for mc in month_columns:
                sim_demand_row[mc] = 0
            ordered_rows.append(sim_demand_row)
        elif step == "Simulated Order":
            # Create new Simulated Order row  
            sim_order_row = step_data["6 Suggested PO Receipt"].to_dict().copy()
            sim_order_row['Step'] = "Simulated Order"
            # Set all month values to 0
            for mc in month_columns:
                sim_order_row[mc] = 0
            ordered_rows.append(sim_order_row)
    
    # Create DataFrame from ordered rows
    enhanced_df = pd.DataFrame(ordered_rows)
    
    # Convert month columns to object type to allow formulas
    for mc in month_columns:
        if mc in enhanced_df.columns:
            enhanced_df[mc] = enhanced_df[mc].astype('object')
    
    # Apply formulas for specific steps
    for i, row in enhanced_df.iterrows():
        step = row['Step']
        current_excel_row = starting_row_num + i + 1  # +1 because Excel rows are 1-based
        
        # Update Ending Inventory with formula
        if step == "7 Ending Inventory":
            for col_idx, mc in enumerate(month_columns):
                excel_col = get_excel_column_name(first_month_col_index + col_idx)
                
                # Calculate absolute row references within this group (fix: use starting_row_num directly, not +1)
                beginning_row = starting_row_num + 0      # "1 Beginning On Hand" (Row 2 in your example)
                demand_row = starting_row_num + 1         # "2 Demand" (Row 3)
                sim_demand_row = starting_row_num + 2     # "Simulated demand" (Row 4)
                forecast_row = starting_row_num + 3       # "3 Forecast" (Row 5)
                po_order_row = starting_row_num + 4       # "4 PO On Order" (Row 6)
                shipment_row = starting_row_num + 5       # "5 Shipment" (Row 7)
                suggested_po_row = starting_row_num + 6   # "6 Suggested PO Receipt" (Row 8)
                sim_order_row = starting_row_num + 7      # "Simulated Order" (Row 9)
                
                # Formula: Beginning + PO + Shipment + Suggested + SimOrder - Demand - SimDemand - Forecast
                # Should be: =E2+E6+E7+E8+E9-E3-E4-E5
                formula = f"={excel_col}{beginning_row}+{excel_col}{po_order_row}+{excel_col}{shipment_row}+{excel_col}{suggested_po_row}+{excel_col}{sim_order_row}-{excel_col}{demand_row}-{excel_col}{sim_demand_row}-{excel_col}{forecast_row}"
                enhanced_df.at[i, mc] = formula
        
        # Update DOC - End of Month with formula
        elif step == "8 DOC - End of Month":
            for col_idx, mc in enumerate(month_columns):
                excel_col = get_excel_column_name(first_month_col_index + col_idx)
                ending_inventory_row = starting_row_num + 8  # "7 Ending Inventory" row (fix: was +9, should be +8)
                
                # Calculate next 3 months columns
                next_3_months_cols = []
                for j in range(1, 4):  # Next 3 months
                    if col_idx + j < len(month_columns):
                        next_col = get_excel_column_name(first_month_col_index + col_idx + j)
                        next_3_months_cols.append(next_col)
                    else:
                        # If no more months, use the last available month
                        next_col = get_excel_column_name(first_month_col_index + len(month_columns) - 1)
                        next_3_months_cols.append(next_col)
        # NEW: Update Beginning On Hand for months 2+ after all formulas are set

                
                # Create sum formula for next 3 months
                sum_parts = []
                for next_col in next_3_months_cols:
                    sum_parts.append(f"{next_col}{demand_row}+{next_col}{sim_demand_row}+{next_col}{forecast_row}")
                
                sum_formula = "+".join(sum_parts)
                
                # Final DOC formula
                formula = f"=IFERROR({excel_col}{ending_inventory_row}/(({sum_formula})/90),\"\")"
                enhanced_df.at[i, mc] = formula
    # NEW: Update Beginning On Hand for months 2+ after all formulas are set
    for i, row in enhanced_df.iterrows():
        if row['Step'] == "1 Beginning On Hand":
            for col_idx, mc in enumerate(month_columns):
                if col_idx > 0:  # Skip first month (keep original value)
                    prev_excel_col = get_excel_column_name(first_month_col_index + col_idx - 1)
                    ending_inventory_row = starting_row_num + 8  # "7 Ending Inventory" row
                    
                    # Set Beginning On Hand = previous month's Ending Inventory
                    # F2=E10, G2=F10, H2=G10, etc.
                    beginning_formula = f"={prev_excel_col}{ending_inventory_row}"
                    enhanced_df.at[i, mc] = beginning_formula
            break  # Only need to update one "Beginning On Hand" row per group
    # Add TDOC row at the end
    tdoc_row = {}
    for col in enhanced_df.columns:
        if col in ['ProductCategory', 'ItemCode', 'SiteCode']:
            tdoc_row[col] = head[col]
        elif col == 'Step':
            tdoc_row[col] = "9 TDOC"
        elif col in month_columns:
            tdoc_row[col] = days_value
        else:
            tdoc_row[col] = pd.NA
    
    # Use pd.concat with proper handling
    tdoc_df = pd.DataFrame([tdoc_row], columns=enhanced_df.columns)
    enhanced_df = pd.concat([enhanced_df, tdoc_df], ignore_index=True)
    
    return enhanced_df

def process_with_target_days(mrp_filename):
    """Process MRP file with target days to create final report with enhanced logic"""
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
        
        # Build final DataFrame with enhanced logic
        final_parts = []
        fallback_used = []  # collect missing matches
        current_row = 2  # Starting row in Excel (after header)
        
        print(f"\n🔄 Processing {len(mrp_df.groupby(['_ITEM_KEY', '_SITE_KEY']))} unique item-site combinations...")
        
        for (item_key, site_key), group in mrp_df.groupby(["_ITEM_KEY", "_SITE_KEY"], sort=False):
            if (item_key, site_key) in target_map:
                days_value = target_map[(item_key, site_key)]
            else:
                days_value = 75
                fallback_used.append((group.iloc[0]["ItemCode"], group.iloc[0]["SiteCode"]))
            
            # Create enhanced rows with correct formulas based on current row position
            enhanced_group = create_enhanced_rows(group, month_columns, days_value, current_row)
            final_parts.append(enhanced_group)
            
            # Update current row for next group (11 rows per group: 8 original + 2 new + 1 TDOC)
            current_row += len(enhanced_group)
        
        final_df = pd.concat(final_parts, ignore_index=True)
        
        # Remove helper columns
        final_df = final_df.drop(columns=["_ITEM_KEY", "_SITE_KEY"])
        
        # Save file with today's date
        today = datetime.now().strftime("%Y-%m-%d")
        output_filename = f"MRP with Target Days_{today}.csv"
        final_df.to_csv(output_filename, index=False)
        
        # Reporting
        print(f"\n✅ Enhanced final file saved as '{output_filename}'")
        print(f"📊 Final records: {len(final_df)}")
        print(f"📊 Unique item-site combinations: {len(final_parts)}")
        print(f"🆕 Added features:")
        print(f"   ✅ Simulated demand rows (after 2 Demand)")
        print(f"   ✅ Simulated Order rows (after 6 Suggested PO Receipt)")
        print(f"   ✅ Formula-based Ending Inventory calculations with absolute references")
        print(f"   ✅ Formula-based DOC - End of Month calculations with next 3 months logic")
        
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
        import traceback
        traceback.print_exc()

# --------- Main Function ---------

def main():
    print("🚀 ENHANCED StockIQ MRP Report Generator with Target Days")
    print("="*70)
    print("STEP 1: Fetch data from StockIQ API and create MRP CSV")
    print("STEP 2: Process MRP CSV with enhanced logic:")
    print("        ✅ Add Simulated Demand rows")
    print("        ✅ Add Simulated Order rows")
    print("        ✅ Formula-based Ending Inventory")
    print("        ✅ Formula-based DOC calculations")
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
                # STEP 2: Process with target days and enhanced logic
                print("\n" + "🟢" * 20 + " STEP 2 " + "🟢" * 20)
                process_with_target_days(mrp_filename)
                today = datetime.now().strftime("%Y-%m-%d")
                print(f"\n🎉 ENHANCED PROCESS COMPLETED!")
                print(f"📁 Check 'MRP with Target Days_{today}.csv' for the final result with:")
                print(f"   🆕 Simulated demand & order rows")
                print(f"   📊 Correct Excel formula references")
            else:
                print("\n❌ Failed to save MRP file, cannot proceed to Step 2")
        else:
            print("\n❌ No data to process after filtering.")
    else:
        print("❌ Failed to retrieve report data from API")

if __name__ == "__main__":
    main()