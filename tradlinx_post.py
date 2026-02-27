import pandas as pd
import requests

def post_tracking_data():
    # Read Excel file
    try:
        tracking_data = pd.read_excel('Tradlinx Tracking Data.xlsm', sheet_name='Sheet1')
    except FileNotFoundError:
        print("❌ Error: Excel file 'Tradlinx Tracking Data.xlsm' not found.")
        return
    except Exception as e:
        print(f"❌ Error reading Excel file: {e}")
        return

    # Drop rows missing essential info
    tracking_data = tracking_data.dropna(subset=["bl_no", "line_cd"])

    # Prepare payload with cleaned inputs
    payload = []
    for _, row in tracking_data.iterrows():
        bl_no = str(row["bl_no"]).strip()
        line_cd = str(row["line_cd"]).strip()
        cust_order_id = str(row["cust_order_id"]).strip() if pd.notna(row["cust_order_id"]) else None

        payload.append({
            "bl_no": bl_no,
            "cust_order_id": cust_order_id,
            "line_cd": line_cd
        })

    if not payload:
        print("⚠️ No valid tracking data found to send.")
        return

    # API configuration
    url = 'https://api.tradlinx.com/partners/track/v2/cargo-tracks/tracking'
    headers = {
        'tx-clientid': 'EarthRated',
        'tx-apikey': 'MjVmMTQzN2QtMzRlMS0zMjA5LWJlZjYtZTRiNjEwOWQ5Nzkw',
        'Content-Type': 'application/json'
    }

    # Send request
    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()  # Raise error for bad HTTP status codes
        print(f"✅ Success! Status Code: {response.status_code}")
        print("🔄 API Response:", response.json())
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {e}")
        if response:
            print(f"Status Code: {response.status_code}")
            print("Response:", response.text)

if __name__ == "__main__":
    post_tracking_data()
