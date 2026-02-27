import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import date
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')

# Google Sheets Configuration
CREDENTIAL_FILE = 'credential.json'
WORKBOOK_NAME = 'Landed Cost'
SHEET_NAME = 'Landed Cost'

# Setup Google Sheets connection
def setup_google_sheets():
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    
    credentials = Credentials.from_service_account_file(CREDENTIAL_FILE, scopes=scope)
    gc = gspread.authorize(credentials)
    
    # Force a fresh connection
    logging.info("Forcing fresh Google Sheets connection...")
    
    return gc

# Function to check sheet status and identify issues
def check_sheet_status(gc, workbook_name, sheet_names):
    try:
        workbook = gc.open(workbook_name)
        logging.info(f"Workbook ID: {workbook.id}")
        logging.info(f"Workbook URL: {workbook.url}")
        
        for sheet_name in sheet_names:
            sheet = workbook.worksheet(sheet_name)
            
            # Get the first row (headers)
            headers = sheet.row_values(1)
            print(f"\n=== {sheet_name} Headers ===")
            print(f"Total columns: {len(headers)}")
            print(f"Headers: {headers}")
            
            # Check for duplicates
            header_counts = {}
            for header in headers:
                header_counts[header] = header_counts.get(header, 0) + 1
            
            duplicates = {k: v for k, v in header_counts.items() if v > 1}
            if duplicates:
                print(f"🚨 DUPLICATE HEADERS FOUND: {duplicates}")
            else:
                print("✅ No duplicate headers")
                
    except Exception as e:
        print(f"Error checking sheet status: {e}")

# Robust function to read multiple sheets from the same workbook
def read_multiple_sheets(gc, workbook_name, sheet_names):
    sheets_data = {}
    try:
        workbook = gc.open(workbook_name)
        for sheet_name in sheet_names:
            print(f"Processing sheet: {sheet_name}")
            sheet = workbook.worksheet(sheet_name)
            
            # Always use get_all_values() to avoid the duplicate header issue
            all_values = sheet.get_all_values()
            
            if not all_values:
                print(f"Warning: {sheet_name} is empty")
                sheets_data[sheet_name] = pd.DataFrame()
                continue
            
            # Get headers and data
            raw_headers = all_values[0]
            data_rows = all_values[1:] if len(all_values) > 1 else []
            
            # Make headers unique
            unique_headers = []
            header_count = {}
            
            for i, header in enumerate(raw_headers):
                clean_header = str(header).strip()
                if clean_header == '':
                    clean_header = f'Unnamed_Column_{i}'
                    
                if clean_header in header_count:
                    header_count[clean_header] += 1
                    unique_headers.append(f"{clean_header}_{header_count[clean_header]}")
                else:
                    header_count[clean_header] = 0
                    unique_headers.append(clean_header)
            
            # Create DataFrame
            df = pd.DataFrame(data_rows, columns=unique_headers)
            sheets_data[sheet_name] = df
            
            print(f"✅ Successfully loaded {len(df)} rows from '{sheet_name}'")
            print(f"   Columns: {list(df.columns)}")
            
        return sheets_data
        
    except Exception as e:
        print(f"❌ Error reading multiple sheets: {e}")
        import traceback
        traceback.print_exc()
        return None

# Function to convert string numbers to float, handle empty strings and non-numeric values
def safe_float_conversion(value, default=0):
    try:
        if value == '' or value is None:
            return default
        return float(str(value).replace(',', ''))  # Remove commas if present
    except (ValueError, TypeError):
        return default

# NEW: Function to convert percentage strings to decimal float
def safe_percentage_conversion(value, default=0):
    try:
        if value == '' or value is None:
            return default
        # Remove % sign and convert to decimal
        value_str = str(value).replace('%', '').replace(',', '').strip()
        if value_str == '':
            return default
        return float(value_str) / 100  # Convert percentage to decimal
    except (ValueError, TypeError):
        return default

# Function to convert string numbers to int, handle empty strings and non-numeric values
def safe_int_conversion(value, default=0):
    try:
        if value == '' or value is None:
            return default
        return int(float(str(value).replace(',', '')))  # Remove commas, convert to float first then int
    except (ValueError, TypeError):
        return default

# Initialize date and input parameters
current_date = date.today()
formatted_date = current_date.strftime("%Y-%m-%d")  
input_number = 2272
customs_brokerage_fee_local_currency = 170
customer_fob_cost_usd = 0
additional_duty = 0
customize_input_occean_rate = 5602
customize_input_drayage_rate = 2200
exchange_rate_usd_to_local_currency = 1
# NEW: Add the Additional Fee input field
include_additional_fee = True  # Set to True to include, False to exclude

# Setup Google Sheets connection
print("Connecting to Google Sheets...")
logging.info(f"Script started at {datetime.now()}")
logging.info(f"Connecting to workbook: {WORKBOOK_NAME}")

gc = setup_google_sheets()

# Check sheet status first
print("Checking sheet status...")
required_sheets = ['Landed Cost', 'SKU_dictionary', 'Duty']
check_sheet_status(gc, WORKBOOK_NAME, required_sheets)

# Read all required sheets from Google Sheets
print("Reading data from Google Sheets...")
sheets_data = read_multiple_sheets(gc, WORKBOOK_NAME, required_sheets)

if sheets_data is None:
    print("Failed to read Google Sheets data. Exiting...")
    exit(1)

# Assign sheet data to variables (equivalent to the original Excel parsing)
quantities = sheets_data['Landed Cost']
sku_dictionary = sheets_data['SKU_dictionary']
duty_data = sheets_data['Duty']

print(f"Successfully loaded {len(quantities)} rows from 'Landed Cost' sheet")
print(f"Successfully loaded {len(sku_dictionary)} rows from 'SKU_dictionary' sheet")
print(f"Successfully loaded {len(duty_data)} rows from 'Duty' sheet")

# Convert numeric columns in quantities to proper data types
numeric_columns = ['Internal Id', 'Volume', 'Weight', 'Qty', 'Price', 'Ocean Shipping Cost', 'Drayage Cost', 'Additional Fee']
for col in numeric_columns:
    if col in quantities.columns:
        quantities[col] = quantities[col].apply(lambda x: safe_float_conversion(x))

# Convert Internal Id to int specifically
if 'Internal Id' in quantities.columns:
    quantities['Internal Id'] = quantities['Internal Id'].apply(lambda x: safe_int_conversion(x))

quantities_corrected = quantities.copy()

# Get warehouse information
warehouse = ""
for _, item in quantities_corrected.iterrows():
    if input_number == item['Internal Id']:
        warehouse = item['Destination WH']
        break

print(f"Warehouse found: {warehouse}")

# Load SKU_dictionary to get category mapping
try:
    # Try to find the category column - adjust the column name as needed
    category_columns = [col for col in sku_dictionary.columns if 'category' in col.lower()]
    if category_columns:
        category_column = category_columns[0]
    else:
        # Fallback to the 11th column (index 10, equivalent to column K)
        if len(sku_dictionary.columns) > 10:
            category_column = sku_dictionary.columns[10]
        else:
            print("Warning: Could not find category column, using last column")
            category_column = sku_dictionary.columns[-1]
    
    sku_to_category_mapping = dict(zip(sku_dictionary['Item'], sku_dictionary[category_column]))
    print(f"Using category column: {category_column}")
    
except Exception as e:
    print(f"Error creating SKU mapping: {e}")
    sku_to_category_mapping = {}

# Add category information to quantities_corrected
quantities_corrected['Category'] = quantities_corrected['Item'].map(sku_to_category_mapping)

# Load duty rates - IMPROVED LOGIC FOR GOOGLE SHEETS
print("\n=== DEBUG: Processing Duty Rates ===")
# Check if the first column contains warehouse names
first_col_values = duty_data.iloc[:, 0].tolist()
print("First column values:", first_col_values)

# Try different approaches to set the index
if 'warehouse' in duty_data.columns[0].lower() or any(warehouse_name in str(first_col_values) for warehouse_name in ['Progressive UK', 'Source Montebello', 'Source NJ', 'Northland Goreway', 'Rhenus Netherlands', 'Golden State FC LLC (AGL)']):
    duty_data_indexed = duty_data.set_index(duty_data.columns[0])
else:
    # If first column doesn't contain warehouse names, look for a warehouse column
    warehouse_cols = [col for col in duty_data.columns if 'warehouse' in col.lower()]
    if warehouse_cols:
        duty_data_indexed = duty_data.set_index(warehouse_cols[0])
    else:
        print("Warning: Could not find warehouse column in duty data")
        duty_data_indexed = duty_data.set_index(duty_data.columns[0])

print("Duty data indexed columns:", duty_data_indexed.columns.tolist())
print("Duty data indexed index:", duty_data_indexed.index.tolist())

# FIXED: Convert duty rate values to float using percentage conversion
for col in duty_data_indexed.columns:
    duty_data_indexed[col] = duty_data_indexed[col].apply(lambda x: safe_percentage_conversion(x))

duty_rates_uk = {}
duty_rates_montebello = {}
duty_rates_nj = {}
duty_rates_northland = {}
duty_rates_Rhenus = {}
duty_rates_AGL = {}

for category in duty_data_indexed.columns:
    try:
        duty_rates_uk[category] = duty_data_indexed.at['Progressive UK', category]
        duty_rates_montebello[category] = duty_data_indexed.at['Source Montebello', category]
        duty_rates_nj[category] = duty_data_indexed.at['Source NJ', category]
        duty_rates_northland[category] = duty_data_indexed.at['Northland Goreway', category]
        duty_rates_Rhenus[category] = duty_data_indexed.at['Rhenus Netherlands', category]
        duty_rates_AGL[category] = duty_data_indexed.at['Golden State FC LLC (AGL)', category]
    except KeyError as e:
        print(f"Warning: Could not find duty rate for category {category}: {e}")
        duty_rates_uk[category] = 0
        duty_rates_montebello[category] = 0
        duty_rates_nj[category] = 0
        duty_rates_northland[category] = 0
        duty_rates_Rhenus[category] = 0
        duty_rates_AGL[category] = 0

duty_rates = {
    'Progressive UK': duty_rates_uk,
    'Source Montebello': duty_rates_montebello,
    'Source NJ': duty_rates_nj,
    'Northland Goreway': duty_rates_northland,
    'Rhenus Netherlands': duty_rates_Rhenus,
    'Golden State FC LLC (AGL)': duty_rates_AGL,
}

print("====================================")

# Warehouse-specific calculations
extra_calculation = False
biggest_duty = 0
needCalculateCurrency = False
isUSWarehouse = False

if warehouse in ["Progressive UK", "Rhenus Netherlands"]:
    extra_calculation = True
if warehouse in["Progressive UK", "Rhenus Netherlands","Northland Goreway"]:
    needCalculateCurrency = True
if warehouse in["Source Montebello","Source NJ","Golden State FC LLC (AGL)"]:
    isUSWarehouse = True

# Filter quantities for the specific input_number
filtered_quantities = quantities_corrected[quantities_corrected['Internal Id'] == input_number]
total_volume = filtered_quantities['Volume'].sum()
total_weight = filtered_quantities['Weight'].sum()

print(f"Found {len(filtered_quantities)} items for Internal Id {input_number}")
print(f"Total Volume: {total_volume}, Total Weight: {total_weight}")

# Determine calculation basis
calculation_basis = 'volume' if total_volume > 65 else 'weight'
total_basis = total_volume if calculation_basis == 'volume' else total_weight

recalculated_costs = []
catalogues = set()
duty_total = 0
total_price = 0
total_price_usd = 0
totalCost = 0

# Calculate total price first - using actual price from each row
price_column = 'Price'
for _, item in filtered_quantities.iterrows():
    unit_price = safe_float_conversion(item[price_column] if price_column in item else 0)
    qty = safe_int_conversion(item['Qty'])
    total_price += qty * unit_price
    total_price_usd += qty * unit_price

# Debug: Check unit prices for filtered items
print("=== DEBUG: Unit Price Check ===")
for _, item in filtered_quantities.iterrows():
    unit_price = safe_float_conversion(item[price_column] if price_column in item else 0)
    qty = safe_int_conversion(item['Qty'])
    print(f"Item: {item['Item']}, Unit Price: {unit_price}, Qty: {qty}, Total: {unit_price * qty}")

print(f"Total Price: {total_price}")
print("================================")

# Check if total_price is zero to avoid division by zero
if total_price == 0:
    print("Warning: Total price is 0. This might be due to missing unit prices.")
    print("Using equal proportions for FOB calculations due to zero total price.")
    use_equal_proportions = True
else:
    use_equal_proportions = False

# First pass: Calculate duty_total for Rhenus Netherlands broker's advancement fee
temp_duty_total = 0
for _, item in filtered_quantities.iterrows():
    proportion = (safe_float_conversion(item['Volume']) if calculation_basis == 'volume' else safe_float_conversion(item['Weight'])) / total_basis
    unit_price = safe_float_conversion(item[price_column] if price_column in item else 0)
    qty = safe_int_conversion(item['Qty'])
    category = sku_to_category_mapping.get(item['Item'], '')
    duty_rate = safe_float_conversion(duty_rates[warehouse].get(category, 0))
    
    # DEBUG: Print detailed duty calculation
    print(f"\n=== DEBUG: Duty Calculation for {item['Item']} ===")
    print(f"Category: {category}")
    print(f"Warehouse: {warehouse}")
    print(f"Duty rate for {category} at {warehouse}: {duty_rate}")
    print(f"Unit price: {unit_price}, Qty: {qty}")
    print(f"item_specific_duty calculation: {qty} * {unit_price} * {duty_rate} = {qty * unit_price * duty_rate}")
    print("==============================================")
    
    item_specific_duty = qty * unit_price * duty_rate
    brokerage_fee_allocation = customs_brokerage_fee_local_currency * proportion
    additional_duty_allocation = additional_duty * proportion
    us_processing_fee_allocation = 0
    
    if isUSWarehouse:
        us_processing_fee_allocation = qty * unit_price * 0.4714 / 100
    
    # Calculate base duty cost
    if warehouse == "Progressive UK":
        if use_equal_proportions or total_price == 0:
            fob_proportion = 1.0 / len(filtered_quantities)
        else:
            fob_proportion = unit_price * qty / total_price
        fob_cost_allocation = customer_fob_cost_usd * duty_rate * fob_proportion
        final_duty_cost_usd = fob_cost_allocation + item_specific_duty
    elif warehouse == "Rhenus Netherlands":
        fob_weight_proportion = safe_float_conversion(item['Weight'])/total_weight
        fob_cost_allocation = customer_fob_cost_usd * duty_rate * fob_weight_proportion
        final_duty_cost_usd = fob_cost_allocation + item_specific_duty 
    else:
        final_duty_cost_usd = item_specific_duty + us_processing_fee_allocation
    
    # Convert to local currency
    if needCalculateCurrency:
        final_duty_cost_usd_convert_to_local = final_duty_cost_usd * exchange_rate_usd_to_local_currency
        final_duty_cost_usd_to_local = final_duty_cost_usd_convert_to_local + brokerage_fee_allocation + additional_duty_allocation
    else:
        final_duty_cost_usd_to_local = final_duty_cost_usd + brokerage_fee_allocation + additional_duty_allocation
    
    temp_duty_total += (final_duty_cost_usd_to_local - brokerage_fee_allocation)
    print(f"temp_duty_total: {temp_duty_total}")
    print(f"final_duty_cost_usd_to_local: {final_duty_cost_usd_to_local}")

# Calculate broker's advancement fee for Rhenus Netherlands
duty_advancement_fee = 0
if warehouse == "Rhenus Netherlands":
    duty_advancement_fee = temp_duty_total * 0.035  # 3.5%
    print(f"Duty Advancement Fee (3.5%): {duty_advancement_fee}")
    print(f"Duty Total + Advancement Fee: {duty_advancement_fee + temp_duty_total}")

# Print Additional Fee information if enabled
print("=== Additional Fee Information ===")
print(f"Include Additional Fee: {include_additional_fee}")
additional_fee_column = 'Additional Fee'

# FIXED: Initialize total_additional_fee regardless of include_additional_fee setting
total_additional_fee = 0
if include_additional_fee:
    print("Additional Fee will be included in the output")
    for _, item in filtered_quantities.iterrows():
        additional_fee = safe_float_conversion(item[additional_fee_column] if additional_fee_column in item else 0)
        if additional_fee > 0:
            print(f"Item {item['Item']} (PO# {item['PO#']}): Additional Fee = {additional_fee}")
            total_additional_fee += additional_fee
    print(f"Total Additional Fee: {total_additional_fee}")
else:
    print("Additional Fee will NOT be included in the output")
print("====================================")

# Main calculation loop with advancement fee allocation
for _, item in filtered_quantities.iterrows():    
    proportion = (safe_float_conversion(item['Volume']) if calculation_basis == 'volume' else safe_float_conversion(item['Weight'])) / total_basis
    fob_weight_proportion = safe_float_conversion(item['Weight'])/total_weight
    unit_price = safe_float_conversion(item[price_column] if price_column in item else 0)
    qty = safe_int_conversion(item['Qty'])
    
    print(f"Item: {item['Item']}, PO#: {item['PO#']}, Unit Price: {unit_price}, Proportion: {proportion:.2%}, FOB Weight Proportion: {fob_weight_proportion:.2%}")
    print('unit price -'+ str(unit_price))
    print('unit total usd - ' + str(qty * unit_price))
    
    # Get category from SKU_dictionary mapping
    category = sku_to_category_mapping.get(item['Item'], '')
    
    # Collect catalogues and find the biggest duty rate if extra calculations are needed
    if extra_calculation:
        duty_rate = safe_float_conversion(duty_rates[warehouse].get(category, 0))
        catalogues.add(category)
        
    totalCost += qty * unit_price
    
    # Handle FOB proportion calculation with zero total_price protection
    if warehouse == "Progressive UK":
        if use_equal_proportions or total_price == 0:
            fob_proportion = 1.0 / len(filtered_quantities)
        else:
            fob_proportion = unit_price * qty / total_price
    
    if warehouse == "Rhenus Netherlands":
        fob_weight_proportion = safe_float_conversion(item['Weight'])/total_weight
    
    # Get duty rate using category from SKU_dictionary
    duty_rate = safe_float_conversion(duty_rates[warehouse].get(category, 0))
    item_specific_duty = qty * unit_price * duty_rate
    brokerage_fee_allocation = customs_brokerage_fee_local_currency * proportion
    additional_duty_allocation = additional_duty * proportion
    us_processing_fee_allocation = 0
    
    if isUSWarehouse:
       us_processing_fee_allocation = qty * unit_price * 0.4714 / 100
    
    # Calculate duty advancement fee allocation for Rhenus Netherlands
    duty_advancement_fee_allocation = 0
    if warehouse == "Rhenus Netherlands":
        duty_advancement_fee_allocation = duty_advancement_fee * fob_weight_proportion
        print(f"Duty Advancement Fee Allocation for {item['Item']} (PO# {item['PO#']}): {duty_advancement_fee_allocation}")
    
    # Determine whether to apply extra calculations based on the warehouse
    if warehouse == "Progressive UK":
        fob_cost_allocation = customer_fob_cost_usd * duty_rate * fob_proportion
        final_duty_cost_usd = fob_cost_allocation + item_specific_duty
    elif warehouse == "Rhenus Netherlands":
        fob_cost_allocation = customer_fob_cost_usd * duty_rate * fob_weight_proportion
        final_duty_cost_usd = fob_cost_allocation + item_specific_duty 
    else:
       final_duty_cost_usd = item_specific_duty + us_processing_fee_allocation
     
    # Convert final duty cost to local currency
    if needCalculateCurrency:
        final_duty_cost_usd_convert_to_local = final_duty_cost_usd * exchange_rate_usd_to_local_currency
        final_duty_cost_usd_to_local = final_duty_cost_usd_convert_to_local + brokerage_fee_allocation + additional_duty_allocation+duty_advancement_fee_allocation
    else:
        final_duty_cost_usd_to_local = final_duty_cost_usd + brokerage_fee_allocation + additional_duty_allocation
    
    final_ocean_rate = proportion * customize_input_occean_rate
    final_drayage_rate = proportion * customize_input_drayage_rate
    
    duty_total += (final_duty_cost_usd_to_local - brokerage_fee_allocation)
    
    # Extract Additional Fee from the Additional Fee column
    additional_fee = safe_float_conversion(item[additional_fee_column] if additional_fee_column in item else 0)
    
    recalculated_costs.append({
        'PO#': item['PO#'],
        'Item': item['Item'],
        'Category': category,
        'Quantity': qty,
        'Unit Price': unit_price,
        'Total Price USD': total_price_usd,
        'Ocean Freight Cost USD': final_ocean_rate,
        'Drayage Cost USD': final_drayage_rate,
        'Final Duty Cost Local Currency': final_duty_cost_usd_to_local,
        'Duty Advancement Fee Allocation': duty_advancement_fee_allocation,
        'Additional Fee': additional_fee,
    })

print('totalCost - ' + str(totalCost))
print('total price usd - ' + str(total_price_usd))
print('duty total - ' + str(duty_total))

# Generate output files
restructured_data = []
input_number_str = str(input_number)
currency = "US Dollar"
if warehouse == "Northland Goreway":
    currency = "CAN"
elif warehouse == "Progressive UK":
    currency = "British pound"
elif warehouse == "Rhenus Netherlands":
    currency = "Euro"

total = 0
for cost in recalculated_costs:
    total += cost['Final Duty Cost Local Currency']
    
    # Base restructured data (always included)
    base_data = [
        {'Internal ID': input_number_str, 'Shipment Number': "INBSHIP" + input_number_str, 'Items - Item': cost['Item'], 'Items - PO': cost['PO#'], 'PO # Line': "PO#"+ cost['PO#'], "PO#ItemLine": "PO#"+ cost['PO#'] + " " + cost['Item'], "Landed Cost - Allocation Method" : "Quantity", 'Landed Cost - Amount': cost['Ocean Freight Cost USD'], "Landed Cost - Cost Category" : "Inbound Freight", "Landed Cost - Currency": "US Dollar", "Landed Cost - Effective Date" : formatted_date},
        {'Internal ID': input_number_str, 'Shipment Number': "INBSHIP" + input_number_str, 'Items - Item': cost['Item'], 'Items - PO': cost['PO#'], 'PO # Line': "PO#"+ cost['PO#'], "PO#ItemLine": "PO#"+ cost['PO#'] + " " + cost['Item'], "Landed Cost - Allocation Method" : "Quantity", 'Landed Cost - Amount': cost['Drayage Cost USD'], "Landed Cost - Cost Category" : "Inbound Freight", "Landed Cost - Currency": "US Dollar", "Landed Cost - Effective Date" : formatted_date},
        {'Internal ID': input_number_str, 'Shipment Number': "INBSHIP" + input_number_str, 'Items - Item': cost['Item'], 'Items - PO': cost['PO#'], 'PO # Line': "PO#"+ cost['PO#'], "PO#ItemLine": "PO#"+ cost['PO#'] + " " + cost['Item'], "Landed Cost - Allocation Method" : "Quantity", 'Landed Cost - Amount': cost['Final Duty Cost Local Currency'], "Landed Cost - Cost Category" : "Inbound Duties", "Landed Cost - Currency": currency, "Landed Cost - Effective Date" : formatted_date}
    ]
    
    # Conditionally add Additional Fee if enabled and value > 0
    if include_additional_fee and cost['Additional Fee'] > 0:
        additional_fee_entry = {
            'Internal ID': input_number_str, 
            'Shipment Number': "INBSHIP" + input_number_str, 
            'Items - Item': cost['Item'], 
            'Items - PO': cost['PO#'], 
            'PO # Line': "PO#"+ cost['PO#'], 
            "PO#ItemLine": "PO#"+ cost['PO#'] + " " + cost['Item'], 
            "Landed Cost - Allocation Method" : "Quantity", 
            'Landed Cost - Amount': cost['Additional Fee'], 
            "Landed Cost - Cost Category" : "Additional Fee", 
            "Landed Cost - Currency": currency, 
            "Landed Cost - Effective Date" : formatted_date
        }
        base_data.append(additional_fee_entry)
        print(f"Adding Additional Fee entry for Item {cost['Item']} (PO# {cost['PO#']}): ${cost['Additional Fee']}")
    
    restructured_data.extend(base_data)

restructured_costs_df = pd.DataFrame(restructured_data)
file_path = f"{input_number_str} Landed Cost Extract.csv"
restructured_costs_df.to_csv(file_path, index=False)
file_path = f"{input_number_str} Landed Cost Extract.xlsx"
restructured_costs_df.to_excel(file_path, sheet_name='Sheet 1', index=False)
print(f"Files saved: {input_number_str} Landed Cost Extract.csv and .xlsx")

# FIXED: Create input data with total_additional_fee always defined
input_data = {
    'Input Number': [input_number],
    'Customs Brokerage Fee (Local Currency)': [customs_brokerage_fee_local_currency],
    'Customer FOB Cost (USD)': [customer_fob_cost_usd],
    'Additional Duty': [additional_duty],
    'Customize Input Ocean Rate': [customize_input_occean_rate],
    'Customize Input Drayage Rate': [customize_input_drayage_rate],
    'Exchange Rate (USD to Local Currency)': [exchange_rate_usd_to_local_currency],
    'Include Additional Fee': [include_additional_fee],
    'Total Additional Fee Amount': [total_additional_fee],  # This is now always defined
    'Total Duty without Customs Brokerage Fee': [duty_total],
    'Duty Advancement Fee (3.5%)': [duty_advancement_fee],
}

input_df = pd.DataFrame(input_data)
file_path = f"{input_number_str} Input Ref.csv"
input_df.to_csv(file_path, index=False)
print(f"Input fields saved to {file_path}")

print("\n=== SUMMARY ===")
print(f"Include Additional Fee: {include_additional_fee}")
if include_additional_fee:
    additional_fee_total = sum([cost['Additional Fee'] for cost in recalculated_costs if cost['Additional Fee'] > 0])
    print(f"Total Additional Fee Amount: ${additional_fee_total}")
    additional_fee_count = len([cost for cost in recalculated_costs if cost['Additional Fee'] > 0])
    print(f"Number of items with Additional Fee: {additional_fee_count}")
else:
    print(f"Total Additional Fee Amount set to: $0")
print("================")

logging.info(f"Script completed successfully at {datetime.now()}")