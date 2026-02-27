import requests
import json
from requests_oauthlib import OAuth1

# --- Configuration for TBA / OAuth 1.0 ---
# 1. Account and Record Information (from your previous script)
SANDBOX_ACCOUNT_ID = "4238542-sb1" # IMPORTANT: For the realm, use UNDERSCORE
PURCHASE_ORDER_INTERNAL_ID = "2257296"

# 2. Authentication Credentials (REPLACE THESE PLACEHOLDERS)
# Get these from your Integration record in NetSuite
CONSUMER_KEY = "528a2922fd6848a94d342c545208cff6ddbd2ffa196304d3274525987993574f"
CONSUMER_SECRET = "dca1e73aab0c45a8470b5bc030626f6052c87865ca1af8ed6d8a6a317f1fa81c"

# Get these from the Access Token record you created in NetSuite
TOKEN_ID = "8cf2de061b1b434fba38f7da67b26c66ba590616ef80a1001fbc9f1591afbe26"
TOKEN_SECRET = "ec8d607c25e8dece64fe185980df23cbee077d8c71425d30c97ea205ed427695"

# --- Construct the Request ---
# The base URL for the REST API in your sandbox. For the URL, use a HYPHEN.
api_base_url = f"https://4238542-sb1.suitetalk.api.netsuite.com"

# The full endpoint URL for the PO's line items
endpoint_url = f"{api_base_url}/services/rest/record/v1/purchaseOrder/{PURCHASE_ORDER_INTERNAL_ID}"

# Optional: Specify exactly which fields you want to retrieve for better performance
query_params = {
    "fields": "item,quantity,rate,amount,description,expectedReceiptDate"
}
auth = OAuth1(
    client_key=CONSUMER_KEY,
    client_secret=CONSUMER_SECRET,
    resource_owner_key=TOKEN_ID,
    resource_owner_secret=TOKEN_SECRET,
    signature_method='HMAC-SHA256',
    realm='4238542_SB1',
    signature_type='auth_header'
)

# Optional but recommended headers
headers = {
    "Prefer": "transient",
}

# --- Create OAuth1 Authentication Object ---
# This is the core of Method B. The library handles the complex signature generation.
# Note: The `realm` must be your account ID with an underscore for sandboxes.

# --- Execute the Request ---
print(f"Querying endpoint: {endpoint_url}")

try:
    # We pass the `auth` object here instead of a manual Authorization header.
    # The `requests` library will use it to sign the request correctly.
    response = requests.get(
        endpoint_url,
        auth=auth,
        headers=headers
        # params=query_params
    )

    # Raise an exception for bad status codes (4xx or 5xx)
    response.raise_for_status()

    # If the request was successful, parse and print the JSON response
    po_line_details = response.json()
    print("\n--- API Response ---")
    print(json.dumps(po_line_details, indent=2))

    # Example of how to iterate through the items
    print("\n--- Parsed Line Items ---")
    if 'items' in po_line_details and po_line_details['items']:
        for line in po_line_details['items']:
            item_name = line.get('item', {}).get('refName', 'N/A')
            quantity = line.get('quantity', 0)
            rate = line.get('rate', 0.0)
            print(f"  - Item: {item_name}, Quantity: {quantity}, Rate: {rate:.2f}")
    else:
        print("No line items found for this Purchase Order.")

except requests.exceptions.HTTPError as e:
    print(f"\nHTTP Error: {e.response.status_code} {e.response.reason}")
    # Print the error details from NetSuite's response body
    print(f"Error Details: {e.response.text}")
except requests.exceptions.RequestException as e:
    print(f"\nRequest failed: {e}")

