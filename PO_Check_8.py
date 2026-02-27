import pandas as pd
import numpy as np
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import logging
from typing import Any, List, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Constants for configuration
CREDENTIAL_FILE = 'credential.json'
SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
SOURCE_WORKBOOK = 'PO check'
YANADA_TARGET_WORKBOOK = 'Yanada POs'
AHC_TARGET_WORKBOOK = 'AHC POs'
SHEET_NAMES = {
    'inbound': 'Inbound Shipment Tracker',         # Container data
    'po': 'Purchase Orders Tracking',              # Item-level data
    'detail': 'PO leftover detail',
    'summary': 'Inventory Summary'
}

def authenticate_google_sheets(credential_file: str, scope: List[str]) -> Any:
    """Authenticates with Google Sheets API using a service account."""
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credential_file, scope)
    client = gspread.authorize(credentials)
    logging.info("Authenticated with Google Sheets API.")
    return client

def get_or_create_worksheet(workbook: Any, sheet_title: str, rows: int = 1000, cols: int = 50) -> Any:
    """Retrieves an existing worksheet by title or creates it if not found."""
    try:
        worksheet = workbook.worksheet(sheet_title)
        logging.info(f"Found existing worksheet: {sheet_title}")
    except gspread.exceptions.WorksheetNotFound:
        worksheet = workbook.add_worksheet(title=sheet_title, rows=str(rows), cols=str(cols))
        logging.info(f"Created worksheet: {sheet_title}")
    return worksheet

def convert_columns_to_numeric(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """Converts specified DataFrame columns to numeric, coercing errors and filling missing values with 0."""
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
            logging.debug(f"Converted column '{col}' to numeric.")
    return df

def prepare_dataframes(inbound_sheet: Any, po_sheet: Any) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Loads data from Google Sheets into DataFrames.
    
    - Inbound data: Renames "Weight" and "CBM" to "Weight_inb" and "CBM_inb".
    - Purchase Orders Tracking:
        • Removes aggregate rows.
        • Converts numeric columns.
        • Drops unnecessary columns.
        • Renames "Gross Weight (KG)" (or "Weight") to "PO GW" and "CBM" to "PO CBM".
        • Calculates Unshipped Quantity.
    """
    df_inbound = pd.DataFrame(inbound_sheet.get_all_records())
    df_po = pd.DataFrame(po_sheet.get_all_records())
    logging.info("Data loaded from Google Sheets.")
    df_inbound.rename(columns={"Weight": "Weight_inb", "CBM": "CBM_inb"}, inplace=True)
    df_po["row_in_po"] = df_po.groupby("PO#").cumcount()
    df_po = df_po[df_po["row_in_po"] != 0].drop(columns="row_in_po")
    logging.debug("Removed aggregate rows from PO DataFrame.")
    numeric_cols_po = ["Unit Quantity", "Quantity Received", "Quantity on Shipment", "Qty"]
    df_po = convert_columns_to_numeric(df_po, numeric_cols_po)
    if "Qty" in df_inbound.columns:
        df_inbound = convert_columns_to_numeric(df_inbound, ["Qty"])
    columns_to_drop = ["Net Weight (KG)"]
    df_po.drop(columns=columns_to_drop, errors="ignore", inplace=True)
    if "Gross Weight (KG)" in df_po.columns:
        df_po.rename(columns={"Gross Weight (KG)": "PO GW"}, inplace=True)
        logging.debug("Renamed 'Gross Weight (KG)' to 'PO GW' in PO DataFrame.")
    if "Weight" in df_po.columns and "PO GW" not in df_po.columns:
        df_po.rename(columns={"Weight": "PO GW"}, inplace=True)
    if "CBM" in df_po.columns and "PO CBM" not in df_po.columns:
        df_po.rename(columns={"CBM": "PO CBM"}, inplace=True)
        logging.debug("Renamed 'CBM' to 'PO CBM' in PO DataFrame.")
    if "Unit Quantity" in df_po.columns and "Quantity Received" in df_po.columns:
        df_po["Unshipped Quantity"] = df_po["Unit Quantity"] - df_po["Quantity Received"]
    else:
        df_po["Unshipped Quantity"] = 0
    logging.info("Prepared inbound and PO DataFrames.")
    return df_inbound, df_po

def merge_and_flag_duplicates(df_po: pd.DataFrame, df_inbound: pd.DataFrame) -> pd.DataFrame:
    """
    Merges Purchase Orders Tracking (df_po) and Inbound Shipment Tracker (df_inbound)
    on keys ["PO#", "Item", "Location", "Quantity on Shipments"].
    Flags duplicate rows based on these keys.
    """
    merge_columns = ["PO#", "Item", "Location", "Quantity on Shipments"]
    df_merged = pd.merge(df_po, df_inbound, how="left", on=merge_columns, suffixes=("", "_dup"))
    df_merged["Is_Duplicate"] = df_merged.duplicated(subset=merge_columns, keep=False)
    for col in ["INBSHIP Number", "Status"]:
        df_merged[col] = df_merged.get(col, pd.Series(index=df_merged.index)).fillna("NA")
        logging.debug(f"Filled missing values in column '{col}'.")
    logging.info("Merged DataFrames and flagged duplicates.")
    return df_merged

def build_detail_dataframe(df_merged: pd.DataFrame) -> pd.DataFrame:
    """
    Constructs the detail DataFrame for 'PO leftover detail' from the merged data.
    
    Final column order:
      1. Supplier Name  
      2. PO#  
      3. Production Month  
      4. Location  
      5. Item  
      6. Unshipped Quantity  
      7. PO GW  
      8. PO CBM  
      9. CRD  
     10. INBSHIP Number  
     11. ETD  
     12. Cntr Weight  
     13. Cntr CBM  
     14. Status  
     15. Is_Duplicate
    """
    df_not_shipped = df_merged[df_merged["Unshipped Quantity"] > 0].copy()
    detail_cols = [
        "Supplier Name", "PO#", "Production Month", "Location", "Item", "Unshipped Quantity",
        "PO GW", "PO CBM", "CRD", "INBSHIP Number", "ETD",
        "Weight_inb", "CBM_inb", "Status", "Is_Duplicate"
    ]
    existing_detail_cols = [c for c in detail_cols if c in df_not_shipped.columns]
    df_detail = df_not_shipped[existing_detail_cols].copy()
    if "Weight_inb" in df_detail.columns:
        df_detail.rename(columns={"Weight_inb": "Cntr Weight"}, inplace=True)
    if "CBM_inb" in df_detail.columns:
        df_detail.rename(columns={"CBM_inb": "Cntr CBM"}, inplace=True)
    logging.info("Built detail DataFrame for PO leftover detail.")
    return df_detail

def sort_detail_dataframe(df_detail: pd.DataFrame) -> pd.DataFrame:
    """
    Sorts the 'PO leftover detail' DataFrame:
      1. Rows with "NA" in INBSHIP Number first (sorted by Production Month and PO#),
      2. Then rows with INBSHIP Number and Status "to be shipped",
      3. Then rows with INBSHIP Number and Status "in transit".
    """
    df_detail = df_detail.copy()
    df_detail['original_order'] = df_detail.index
    group1 = df_detail[df_detail["INBSHIP Number"] == "NA"]
    group2 = df_detail[(df_detail["INBSHIP Number"] != "NA") & (df_detail["Status"].str.lower() == "to be shipped")]
    group3 = df_detail[(df_detail["INBSHIP Number"] != "NA") & (df_detail["Status"].str.lower() == "in transit")]
    group1_sorted = group1.sort_values(by=["Production Month", "PO#", "original_order"])
    group2_sorted = group2.sort_values(by=["INBSHIP Number", "original_order"])
    group3_sorted = group3.sort_values(by=["INBSHIP Number", "original_order"])
    df_sorted = pd.concat([group1_sorted, group2_sorted, group3_sorted])
    df_sorted.drop(columns=["original_order"], inplace=True)
    return df_sorted

def build_summary_dataframe(df_detail: pd.DataFrame) -> pd.DataFrame:
    """
    Constructs the summary DataFrame for 'Inventory Summary' by aggregating detail data.
    Aggregation is on: Supplier Name, Location, and Item.
    """
    group_cols = ["Supplier Name", "Location", "Item"]
    def in_transit_qty(group: pd.DataFrame) -> float:
        return group.loc[group["Status"] == "In Transit", "Unshipped Quantity"].sum()
    def unshipped_qty(group: pd.DataFrame) -> float:
        return group.loc[group["Status"].isin(["To Be Shipped", "NA"]), "Unshipped Quantity"].sum()
    df_summary = df_detail.groupby(group_cols).apply(lambda g: pd.Series({
        "In Transit Quantity": in_transit_qty(g),
        "Unshipped Quantity": unshipped_qty(g)
    })).reset_index()
    df_summary["Total on Order Quantity"] = df_summary["In Transit Quantity"] + df_summary["Unshipped Quantity"]
    logging.info("Built summary DataFrame for Inventory Summary.")
    return df_summary

def build_yanada_shipping_dataframe(df_inbound: pd.DataFrame) -> pd.DataFrame:
    """
    Constructs the Yanada Shipping DataFrame from inbound data.
    Selects groups (by Internal Id) where at least one row has Supplier Name "SUNNER GROUP CO., LTD.".
    Renames required columns and adds a blank 'AMS#'.
    """
    groups = df_inbound.groupby("Internal Id")
    list_dfs = [group for id_val, group in groups if (group["Supplier Name"] == "SUNNER GROUP CO., LTD.").any()]
    df_yanada = pd.concat(list_dfs) if list_dfs else pd.DataFrame()
    columns_mapping = {
        "Internal Id": "Internal Id", "Status": "Status", "PO#": "PO#", "Supplier Name": "Supplier Name",
        "Location": "Location", "Item": "Item", "Quantity on Shipments": "Quantity",
        "Total Cases-Line": "Total Cases", "N.W.-Line": "Net Weight", "G.W.-Line": "Gross Weight",
        "CBM-Line": "CBM", "Item Rate": "Item Rate", "Item Amount": "Item Amount", "MBL#": "MBL#",
        "Container#": "Container#", "ETD": "ETD"
    }
    cols_to_keep = [col for col in columns_mapping if col in df_yanada.columns]
    df_yanada = df_yanada[cols_to_keep].copy()
    df_yanada.rename(columns=columns_mapping, inplace=True)
    df_yanada["AMS#"] = ""
    desired_order = ["Internal Id", "Status", "PO#", "Supplier Name", "Location", "Item",
                     "Quantity", "Total Cases", "Net Weight", "Gross Weight", "CBM",
                     "Item Rate", "Item Amount", "MBL#", "AMS#", "Container#", "ETD"]
    df_yanada = df_yanada[[col for col in desired_order if col in df_yanada.columns]]
    return df_yanada

def build_ahc_shipping_dataframe(df_inbound: pd.DataFrame) -> pd.DataFrame:
    """
    Constructs the AHC Shipping DataFrame from inbound data.
    Selects groups (by Internal Id) where the unique Supplier Name is "American Hygienics Corporation".
    Renames required columns and adds a blank 'AMS#'.
    """
    groups = df_inbound.groupby("Internal Id")
    list_dfs = [group for id_val, group in groups if set(group["Supplier Name"].unique()) == {"American Hygienics Corporation"}]
    df_ahc = pd.concat(list_dfs) if list_dfs else pd.DataFrame()
    columns_mapping = {
        "Internal Id": "Internal Id", "Status": "Status", "PO#": "PO#", "Supplier Name": "Supplier Name",
        "Location": "Location", "Item": "Item", "Quantity on Shipments": "Quantity",
        "Total Cases-Line": "Total Cases", "N.W.-Line": "Net Weight", "G.W.-Line": "Gross Weight",
        "CBM-Line": "CBM", "Item Rate": "Item Rate", "Item Amount": "Item Amount", "MBL#": "MBL#",
        "Container#": "Container#", "ETD": "ETD"
    }
    cols_to_keep = [col for col in columns_mapping if col in df_ahc.columns]
    df_ahc = df_ahc[cols_to_keep].copy()
    df_ahc.rename(columns=columns_mapping, inplace=True)
    df_ahc["AMS#"] = ""
    desired_order = ["Internal Id", "Status", "PO#", "Supplier Name", "Location", "Item",
                     "Quantity", "Total Cases", "Net Weight", "Gross Weight", "CBM",
                     "Item Rate", "Item Amount", "MBL#", "AMS#", "Container#", "ETD"]
    df_ahc = df_ahc[[col for col in desired_order if col in df_ahc.columns]]
    return df_ahc

def build_yanada_po_dataframe(po_sheet: Any) -> pd.DataFrame:
    """
    Reads data from "Purchase Orders Tracking" and filters rows where Supplier Name is "SUNNER GROUP CO., LTD.".
    The output DataFrame retains the original sheet's format.
    """
    df_po_original = pd.DataFrame(po_sheet.get_all_records())
    df_po_original["Supplier Name"] = df_po_original["Supplier Name"].astype(str).str.strip()
    df_yanada_po = df_po_original[df_po_original["Supplier Name"] == "SUNNER GROUP CO., LTD."]
    return df_yanada_po

def build_ahc_po_dataframe(po_sheet: Any) -> pd.DataFrame:
    """
    Reads data from "Purchase Orders Tracking" and filters rows where Supplier Name is exactly "American Hygienics Corporation".
    The output DataFrame retains the original sheet's format.
    """
    df_po_original = pd.DataFrame(po_sheet.get_all_records())
    df_po_original["Supplier Name"] = df_po_original["Supplier Name"].astype(str).str.strip()
    df_ahc_po = df_po_original[df_po_original["Supplier Name"] == "American Hygienics Corporation"]
    return df_ahc_po

def update_google_sheet(sheet: Any, df: pd.DataFrame) -> None:
    """
    Clears existing content in a worksheet and updates it with new data from a DataFrame.
    Sanitizes the DataFrame by converting NaN or infinite values to "NA".
    """
    df = df.applymap(lambda x: "NA" if (isinstance(x, float) and (np.isnan(x) or np.isinf(x))) else x)
    sheet.clear()
    headers = df.columns.tolist()
    data = df.values.tolist()
    sheet.update([headers] + data)
    logging.info(f"Updated worksheet '{sheet.title}' with {len(data)} rows.")

def update_shipping_sheet(sheet: Any, new_df: pd.DataFrame) -> None:
    """
    Special update for shipping worksheets ("Yanada Shipping" and "AHC Shipping").
    - Only updates columns A through Q (first 17 columns)
    - Preserves existing non-blank AMS# values based on key: ["Internal Id", "PO#", "Supplier Name", "Item"]
    - Preserves all columns after column Q
    """
    try:
        existing_data = sheet.get_all_records()
        existing_values = sheet.get_all_values()
    except Exception as e:
        logging.error(f"Failed to get existing data: {e}")
        existing_data = []
        existing_values = []

    if existing_data:
        df_existing = pd.DataFrame(existing_data)
        # Create DataFrame with all columns including headers
        df_full = pd.DataFrame(existing_values)
        
        # If there's data, get columns after Q (index 16)
        additional_cols = df_full.iloc[:, 17:] if df_full.shape[1] > 17 else pd.DataFrame()
    else:
        df_existing = pd.DataFrame()
        additional_cols = pd.DataFrame()

    # Handle AMS# preservation logic
    key_cols = ["Internal Id", "PO#", "Supplier Name", "Item"]
    if not df_existing.empty and all(col in df_existing.columns for col in key_cols + ["AMS#"]):
        df_existing = df_existing.drop_duplicates(subset=key_cols)
        df_merge = pd.merge(new_df, df_existing[key_cols + ["AMS#"]],
                          on=key_cols, how="left", suffixes=("", "_old"))
        df_merge["AMS#"] = df_merge.apply(
            lambda row: row["AMS#_old"] if (row["AMS#"] == "" or pd.isna(row["AMS#"])) and pd.notna(row["AMS#_old"])
            else row["AMS#"],
            axis=1
        )
        df_merge.drop(columns=["AMS#_old"], inplace=True)
        new_df = df_merge

    # Sanitize the data
    new_df = new_df.applymap(lambda x: "NA" if (isinstance(x, float) and (np.isnan(x) or np.isinf(x))) else x)

    # Prepare the update data
    headers = new_df.columns.tolist()
    data = new_df.values.tolist()
    update_data = [headers] + data

    # If there are additional columns, append them to the update data
    if not additional_cols.empty:
        # Skip the header row from additional_cols as we already have headers
        additional_data = additional_cols.iloc[1:].values.tolist()
        
        # Extend each row with the additional columns
        for i in range(len(update_data)):
            if i < len(additional_data):
                update_data[i].extend(additional_data[i])
            else:
                # If new_df has more rows than existing data, pad with empty strings
                update_data[i].extend([''] * additional_cols.shape[1])

    # Update only the range needed (A1:Q{last_row})
    num_rows = len(update_data)
    range_name = f'A1:Q{num_rows}'
    sheet.batch_clear([range_name])
    sheet.update(range_name, [row[:17] for row in update_data])  # Only update first 17 columns
    
    logging.info(f"Updated shipping worksheet '{sheet.title}' columns A-Q with {len(data)} rows.")

def main() -> None:
    client = authenticate_google_sheets(CREDENTIAL_FILE, SCOPE)
    # Open source workbook "PO check"
    source_wb = client.open(SOURCE_WORKBOOK)
    
    # Get source worksheets needed for processing
    inbound_ws = source_wb.worksheet(SHEET_NAMES['inbound'])
    po_ws = source_wb.worksheet(SHEET_NAMES['po'])
    
    # Prepare data from inbound and PO worksheets
    df_inbound, df_po = prepare_dataframes(inbound_ws, po_ws)
    df_merged = merge_and_flag_duplicates(df_po, df_inbound)
    
    # Build "PO leftover detail" and "Inventory Summary"
    df_detail = build_detail_dataframe(df_merged)
    df_detail_sorted = sort_detail_dataframe(df_detail)
    df_summary = build_summary_dataframe(df_detail_sorted)
    
    # Build shipping and PO worksheets data from source "PO check"
    df_yanada_shipping = build_yanada_shipping_dataframe(df_inbound)
    df_ahc_shipping = build_ahc_shipping_dataframe(df_inbound)
    df_yanada_po = build_yanada_po_dataframe(po_ws)
    df_ahc_po = build_ahc_po_dataframe(po_ws)
    
     # Update the six worksheets in the source workbook ("PO check")
    detail_sheet = get_or_create_worksheet(source_wb, SHEET_NAMES['detail'])
    summary_sheet = get_or_create_worksheet(source_wb, SHEET_NAMES['summary'])
    yanada_shipping_sheet = get_or_create_worksheet(source_wb, "Yanada Shipping")
    yanada_po_sheet = get_or_create_worksheet(source_wb, "Yanada PO")
    ahc_shipping_sheet = get_or_create_worksheet(source_wb, "AHC Shipping")
    ahc_po_sheet = get_or_create_worksheet(source_wb, "AHC PO")
    
    update_google_sheet(detail_sheet, df_detail_sorted)
    update_google_sheet(summary_sheet, df_summary)
    update_google_sheet(yanada_shipping_sheet, df_yanada_shipping) 
    update_google_sheet(yanada_po_sheet, df_yanada_po)
    update_google_sheet(ahc_shipping_sheet, df_ahc_shipping) 
    update_google_sheet(ahc_po_sheet, df_ahc_po)
    
    # Now update target workbooks:
    # "Yanada POs" gets data from source "Yanada Shipping" and "Yanada PO"
    # "AHC POs" gets data from source "AHC Shipping" and "AHC PO"
    yanada_wb = client.open("Yanada POs")
    ahc_wb = client.open("AHC POs")
    
    yanada_shipping_target = get_or_create_worksheet(yanada_wb, "Yanada Shipping")
    yanada_po_target = get_or_create_worksheet(yanada_wb, "Yanada PO")
    ahc_shipping_target = get_or_create_worksheet(ahc_wb, "AHC Shipping")
    ahc_po_target = get_or_create_worksheet(ahc_wb, "AHC PO")
    
    # We can use update_google_sheet() to copy data from our DataFrames built above
    update_google_sheet(yanada_shipping_target, df_yanada_shipping)
    update_google_sheet(yanada_po_target, df_yanada_po)
    update_shipping_sheet(ahc_shipping_target, df_ahc_shipping)
    update_google_sheet(ahc_po_target, df_ahc_po)
    
    logging.info("Source workbook 'PO check' updated with 'PO leftover detail', 'Inventory Summary', 'Yanada Shipping', 'Yanada PO', 'AHC Shipping', and 'AHC PO'.")
    logging.info("Target workbooks 'Yanada POs' and 'AHC POs' have been updated with the corresponding shipping and PO data.")

if __name__ == "__main__":
    main()
