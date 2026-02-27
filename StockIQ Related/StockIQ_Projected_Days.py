import requests
import json
import pandas as pd
from requests.auth import HTTPBasicAuth
from datetime import datetime
import os


def get_shipping_status_report(base_url, username, password, report_format='json'):
    """
    Get 'Shipping Status & Projected Amounts' report from StockIQ
    """
    endpoint = f"{base_url}/CustomReportProducer"

    params = {
        'customReportName': 'Shipping Status & Projected Amounts',
        'format': report_format
    }

    try:
        print(f"Making request to: {endpoint}")
        print(f"Parameters: {params}")

        response = requests.get(
            endpoint,
            params=params,
            auth=HTTPBasicAuth(username, password),
            headers={
                'Accept': 'application/json' if report_format == 'json' else 'text/csv',
                'Content-Type': 'application/json'
            },
            timeout=60
        )

        print(f"Response Status Code: {response.status_code}")

        response.raise_for_status()

        if report_format == 'json':
            return response.json()
        else:
            return response.text

    except requests.exceptions.RequestException as e:
        print(f"Error fetching report: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Error response: {e.response.text}")
        return None


def format_and_save_shipping_report(report_data, save_directory=None):
    """
    Format 'Shipping Status & Projected Amounts' report and save as CSV
    """

    try:
        # Convert data to DataFrame for processing
        if isinstance(report_data, str):
            from io import StringIO
            df = pd.read_csv(StringIO(report_data))
        elif isinstance(report_data, dict):
            if 'data' in report_data:
                df = pd.DataFrame(report_data['data'])
            else:
                df = pd.DataFrame([report_data])
        elif isinstance(report_data, list):
            df = pd.DataFrame(report_data)
        else:
            df = pd.DataFrame(report_data)

        # --- Supplier Mapping ---
        supplier_mapping = {
            "476": "SUNNER GROUP CO., LTD.",
            "422": "American Hygienics Corporation",
            "13965": "Tianjin Yiyi Hygiene Products Co., Ltd.",
            "13909": "Costech Lab LLC",  # you listed two for 13909, using this as final
            "13558": "DELON LAB. (1990) INC",
            "10453": "Thai Duong Rubber Joint Stock Company"
        }

        # --- Column Mapping from StockIQ to Desired Output ---
        column_mapping = {
            'ErpOrderNumber': 'Erp Order Number',
            'ERPOrderNumber': 'Erp Order Number',
            'SupplyType': 'Supply Type',
            'Item': 'Item',
            'ItemCode': 'Item',
            'Supplier': 'Supplier',
            'Vendor': 'Supplier',
            'DestinationWarehouse': 'Destination Warehouse',
            'SiteCode': 'Destination Warehouse',
            'Quantity': 'Quantity',
            'Qty': 'Quantity',
            'ExpectedDeliveryDate': 'Expected Delivery Date',
            'ETA': 'Expected Delivery Date',
            'ShippingStatus': 'Shipping Status',
            'ProjActualAvailableBeforeArrival': 'Proj Actual Available Before Arrival',
            'ProjDaysBeforeArrival': 'Proj Days Before Arrival',
            'ProjActualAvailableAfterArrival': 'Proj Actual Available After Arrival',
            'ProjDaysAfterArrival': 'Proj Days After Arrival',
            'DateOfOrderCreation': 'Date of Order Creation',
            'OrderCreationDate': 'Date of Order Creation'
        }

        # Rename columns according to mapping
        df.rename(columns=column_mapping, inplace=True)

        # Ensure required columns exist
        required_columns = [
            'Erp Order Number', 'Supply Type', 'Item', 'Supplier',
            'Destination Warehouse', 'Quantity', 'Expected Delivery Date',
            'Shipping Status', 'Proj Actual Available Before Arrival',
            'Proj Days Before Arrival', 'Proj Actual Available After Arrival',
            'Proj Days After Arrival', 'Date of Order Creation'
        ]

        for col in required_columns:
            if col not in df.columns:
                df[col] = ''

        # Keep only those columns in the defined order
        df = df[required_columns]

        # --- Apply Supplier Mapping ---
        df['Supplier'] = df['Supplier'].astype(str).map(supplier_mapping).fillna(df['Supplier'])

        # --- Format Dates ---
        date_cols = ['Expected Delivery Date', 'Date of Order Creation']
        for col in date_cols:
            if col in df.columns:
                try:
                    df[col] = pd.to_datetime(df[col]).dt.strftime('%m/%d/%Y')
                except Exception:
                    pass

        # --- Format numeric columns ---
        numeric_cols = [
            'Quantity', 'Proj Actual Available Before Arrival',
            'Proj Days Before Arrival', 'Proj Actual Available After Arrival',
            'Proj Days After Arrival'
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        # Sort by Expected Delivery Date
        if 'Expected Delivery Date' in df.columns:
            df = df.sort_values('Expected Delivery Date', ignore_index=True)

        # --- Save File ---
        today = datetime.now().strftime("%Y-%m-%d")
        filename = f"Shipping_Status_Projected_Amounts_{today}.csv"

        if save_directory:
            os.makedirs(save_directory, exist_ok=True)
            filepath = os.path.join(save_directory, filename)
        else:
            filepath = filename

        df.to_csv(filepath, index=False, encoding='utf-8', lineterminator='\n')

        print(f"✅ Report saved successfully to: {filepath}")
        print(f"Total rows: {len(df)}")
        print("Column structure preview:")
        print(df.head())

        return filepath

    except Exception as e:
        print(f"Error formatting and saving report: {e}")
        return None


def get_and_save_shipping_status(base_url, username, password, save_directory=None):
    """
    Get and save the 'Shipping Status & Projected Amounts' report
    """

    print("Fetching Shipping Status & Projected Amounts report...")

    report_data = get_shipping_status_report(base_url, username, password, report_format='json')

    if not report_data:
        print("JSON failed, trying CSV format...")
        report_data = get_shipping_status_report(base_url, username, password, report_format='csv')

    if report_data:
        print("Report retrieved successfully!")
        filepath = format_and_save_shipping_report(report_data, save_directory)
        return filepath
    else:
        print("Failed to retrieve report")
        return None


# === USAGE ===
BASE_URL = 'https://earthrated.stockiqtech.net/api'
USERNAME = 'Ying'
PASSWORD = 'Kx5&f}H3F8'

saved_file = get_and_save_shipping_status(BASE_URL, USERNAME, PASSWORD)

if saved_file:
    print(f"\n✅ Success! Report saved to: {saved_file}")
else:
    print("❌ Failed to retrieve and save report")
