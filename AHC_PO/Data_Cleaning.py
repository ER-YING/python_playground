import pandas as pd
import math
import os

# -----------------------------------------------------------------
#   File path definitions (adjust as needed)
# -----------------------------------------------------------------
input_file_summary  = '/Users/yingji/Documents/python/vs_code/python_playground/AHC_PO/Summary.xlsx'
input_file_shipping = '/Users/yingji/Documents/python/vs_code/python_playground/AHC_PO/ShippingTiHi.xlsx'
input_file_sku      = '/Users/yingji/Documents/python/vs_code/python_playground/AHC_PO/Sku Dictionary.xlsx'
output_file         = '/Users/yingji/Documents/python/vs_code/python_playground/AHC_PO/Summary_Cleaned.xlsx'

# -----------------------------------------------------------------
#   Read the input files
# -----------------------------------------------------------------
summary_df  = pd.read_excel(os.path.expanduser(input_file_summary))
shipping_df = pd.read_excel(os.path.expanduser(input_file_shipping))
sku_df      = pd.read_excel(os.path.expanduser(input_file_sku))

# -----------------------------------------------------------------
#   Drop duplicates if needed
# -----------------------------------------------------------------
shipping_df = shipping_df.drop_duplicates(subset=['Item','Location'], keep='first')
summary_df  = summary_df.drop_duplicates(subset=['Item','Location'], keep='first')
sku_df      = sku_df.drop_duplicates(subset=['Item'], keep='first')

# -----------------------------------------------------------------
#   Clean up column names for consistent merges
# -----------------------------------------------------------------
shipping_df.rename(columns={'Cases per pallet': 'cases_per_pallet'}, inplace=True)
sku_df.rename(columns={'units per case/carton': 'units_per_case_carton'}, inplace=True)

summary_df.columns = summary_df.columns.str.strip()
shipping_df.columns = shipping_df.columns.str.strip()
sku_df.columns = sku_df.columns.str.strip()

# -----------------------------------------------------------------
#   Merge summary with ShippingTiHi on (Item, Location)
# -----------------------------------------------------------------
merged_df = pd.merge(
    summary_df, 
    shipping_df[['Item','Location','cases_per_pallet']],
    on=['Item','Location'],
    how='left'
)

# -----------------------------------------------------------------
#   Merge with SKU Dictionary on Item only
# -----------------------------------------------------------------
merged_df = pd.merge(
    merged_df,
    sku_df[['Item','units_per_case_carton']],
    on='Item',
    how='left'
)

# -----------------------------------------------------------------
#   Define function to compute suggested quantity in units
# -----------------------------------------------------------------
def compute_suggested_quantity(row):
    """
    1) Convert 'Quantity' (units) to 'cartons' by dividing by units_per_case_carton.
    2) Convert 'cartons' to 'pallets' by dividing by cases_per_pallet.
    3) If actual pallets (for a positive quantity) is less than 1, set suggested pallets = 1.
    4) Otherwise, use:
         - roundup if fractional part > 0.5,
         - roundown if fractional part < 0.5.
    5) Convert suggested pallets back to units (for 'Suggested Quantity').
    6) 'Quantity Variance' = actual units - suggested units.
    """
    qty_units = row['Quantity']
    upc = row['units_per_case_carton']
    cpp = row['cases_per_pallet']

    # Handle missing or zero gracefully
    if pd.isna(upc) or upc == 0:
        upc = 1
    if pd.isna(cpp) or cpp == 0:
        cpp = 1

    # Convert units -> cartons and then to pallets
    actual_cartons = qty_units / upc
    actual_pallets = actual_cartons / cpp

    # Check if we have a positive quantity that results in less than one pallet.
    if qty_units > 0 and actual_pallets < 1:
        suggested_pallets = 1
    else:
        fractional_part = actual_pallets - math.floor(actual_pallets)
        if math.isclose(fractional_part, 0.0, abs_tol=1e-9):
            suggested_pallets = actual_pallets
        else:
            if fractional_part > 0.5:
                suggested_pallets = math.ceil(actual_pallets)
            else:
                suggested_pallets = math.floor(actual_pallets)

    # Convert suggested pallets back into units
    suggested_units = suggested_pallets * cpp * upc
    quantity_variance = qty_units - suggested_units

    return pd.Series({
        'Actual Pallets': actual_pallets,
        'Suggested Pallets': suggested_pallets,
        'Suggested Quantity': suggested_units,
        'Quantity Variance': quantity_variance
    })

# -----------------------------------------------------------------
#   Apply function to each row and attach new columns
# -----------------------------------------------------------------
results_df = merged_df.apply(compute_suggested_quantity, axis=1)
cleaned_df = pd.concat([merged_df, results_df], axis=1)

# -----------------------------------------------------------------
#   Write to Excel
# -----------------------------------------------------------------
cleaned_df.to_excel(os.path.expanduser(output_file), index=False)
print("data cleaning is done")
