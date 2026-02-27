import pandas as pd
import os
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_UP

# ---------------------------------------------------------
# Helper: Excel-like ROUNDUP (away from zero)
# ---------------------------------------------------------
def rup(x, ndigits=0):
    q = Decimal('1').scaleb(-ndigits)  # 10**-ndigits
    return float(Decimal(str(x)).quantize(q, rounding=ROUND_UP))

# ---------------------------------------------------------
# 1. Define Paths
# ---------------------------------------------------------
input_path_1 = '/Users/yingji/Documents/python/vs_code/python_playground/AHC_PO/Container_Building_Output_6.xlsx'
input_path_2 = '/Users/yingji/Documents/python/vs_code/python_playground/AHC_PO/SKU Dictionary.xlsx'
output_path = '/Users/yingji/Documents/python/vs_code/python_playground/AHC_PO/PO_Upload_Output_2.csv'

# ---------------------------------------------------------
# 2. Define Mappings
#    (Adjust these as needed for your use-case)
# ---------------------------------------------------------
location_mapping = {
    "Northland Goreway": 2,
    "Progressive UK": 9,
    "Rhenus Netherlands": 10,
    "Source Montebello": 107,
    "Source NJ": 109,
    "Yanada": 13,
    "Golden State FC LLC (AGL)": 119
}

shipping_lead_time_mapping = {
    "Northland Goreway": 60,
    "Progressive UK": 60,
    "Rhenus Netherlands": 60,
    "Source Montebello": 30,
    "Source NJ": 60,
    "Golden State FC LLC (AGL)": 40,
    "Yanada": 15
}

vendor_internal_id_mapping = {
    "SUNNER GROUP CO., LTD.": 476,
    "Xiamen Hitita International Trade Co., Ltd": 9061,
    "American Hygienics Corporation": 422,
    "Thai Duong Rubber Joint Stock Company": 10453,
    "DELON LAB. (1990) INC": 13558,
    "Tianjin Yiyi Hygiene Products Co., Ltd.": 13965
}

production_lead_time_mapping = {
    "SUNNER GROUP CO., LTD.": 20,
    "Xiamen Hitita International Trade Co., Ltd": 30,
    "American Hygienics Corporation": 50,
    "Thai Duong Rubber Joint Stock Company": 50,
    "DELON LAB. (1990) INC": 60,
    "Tianjin Yiyi Hygiene Products Co., Ltd.": 50
}

# ---------------------------------------------------------
# 3. Load Data
# ---------------------------------------------------------
try:
    container_data = pd.read_excel(os.path.expanduser(input_path_1))
    sku_dictionary = pd.read_excel(os.path.expanduser(input_path_2), sheet_name="SKU Dictionary")
except Exception as e:
    print(f"Error loading files: {e}")
    exit()

# ---------------------------------------------------------
# 4. Prepare for Output
# ---------------------------------------------------------
output_data = []
today_str = datetime.today().strftime("%Y%m%d")

# Group by "Container#"
container_groups = container_data.groupby("Container#")
external_id_counter = 1

# ---------------------------------------------------------
# 5. Build Rows for the Output CSV
# ---------------------------------------------------------
for container, group in container_groups:
    # Create a unique External ID per container
    external_id = f"{today_str}{external_id_counter}"
    external_id_counter += 1

    for idx, row in group.iterrows():
        try:
            # Extract data from each row
            location = row["Location"]
            payee = row["Supplier"]  # or row["Payee"], depending on your columns
            item = row["Item"]
            box_qty = row["Quantity"]

            # Lookup Payee Price in SKU Dictionary
            payee_price = sku_dictionary.loc[sku_dictionary["Item"] == item, "Payee Price"].values[0]

            # Basic columns
            subsidiary = "Earth Rated"
            location_internal_id = location_mapping.get(location, "Unknown")
            date = datetime.today().strftime("%Y-%m-%d")

            # Requested ship date = today + production lead time
            requested_ship_date = (
                datetime.today() 
                + timedelta(days=production_lead_time_mapping.get(payee, 0))
            ).strftime("%Y-%m-%d")

            # Expected receipt date = requested ship date + shipping lead time
            exp_receipt_date = (
                datetime.strptime(requested_ship_date, "%Y-%m-%d")
                + timedelta(days=shipping_lead_time_mapping.get(location, 0))
            ).strftime("%Y-%m-%d")

            production_month = ""
            memo = ""
            status = "open"
            vendor_internal_id = vendor_internal_id_mapping.get(payee, "Unknown")

            # Lookup internal ID for item from SKU Dictionary
            sku_internal_id = sku_dictionary.loc[sku_dictionary["Item"] == item, "Internal ID"].values[0]

            # Calculate UNIT_COST_Orig (amended only for Sunner)
            if payee == "SUNNER GROUP CO., LTD.":
                if location == "Progressive UK":
                    unit_cost_orig = rup(payee_price * 0.85, 3)
                elif location in ["Northland Goreway", "Rhenus Netherlands"]:
                    unit_cost_orig = rup(payee_price * 0.90, 3)
                elif location in ["Source Montebello", "Source NJ", "Golden State FC LLC (AGL)"]:
                    item_u = str(item).upper()
                    if any(k in item_u for k in ("BG0051", "BG0070", "BG0071")):
                        unit_cost_orig = rup(payee_price * 1, 3)
                    elif any(k in item_u for k in ("BG0052", "BG0053", "BG0054", "BG0055")):
                        unit_cost_orig = rup(payee_price * 0.75, 3)
                    elif "DP" in item_u:
                        unit_cost_orig = rup(payee_price * 0.77, 3)
                    else:
                        unit_cost_orig = rup(payee_price * 0.79, 3)
                else:
                    unit_cost_orig = payee_price
            else:
                unit_cost_orig = payee_price

            # Total amount: roundup only for Sunner, keep existing behavior for others
            if payee == "SUNNER GROUP CO., LTD.":
                total_amount = rup(box_qty * unit_cost_orig, 2)
            else:
                total_amount = round(box_qty * unit_cost_orig, 2)

            # Append row to output_data
            output_data.append([
                subsidiary, location, location_internal_id, date, exp_receipt_date, external_id,
                production_month, memo, requested_ship_date, status, payee, vendor_internal_id,
                item, sku_internal_id, box_qty, unit_cost_orig, total_amount
            ])
        except Exception as e:
            print(f"Error processing row {idx + 1}: {e}")

# ---------------------------------------------------------
# 6. Convert to DataFrame & Write CSV
# ---------------------------------------------------------
output_columns = [
    "Subsidiary", "Location", "Location InternalID", "Date", "Exp Receipt Date",
    "External ID", "Production Month", "Memo", "Requested Ship Date", "Status", 
    "Payee", "Vendor InternalID", "ITEM", "Item InternalID", 
    "BOX QTY", "UNIT COST_Orig", "Total Amount $"
]

output_df = pd.DataFrame(output_data, columns=output_columns)

try:
    output_df.to_csv(os.path.expanduser(output_path), index=False, encoding='utf-8-sig')
    print("Output CSV file created successfully.")
except Exception as e:
    print(f"Error saving output CSV file: {e}")
