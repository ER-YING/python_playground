import requests

def track_package(tracking_identifier, api_key):
    url = f"https://api.oecgroup.ca/v1/Tracking/{tracking_identifier}"
    headers = {
        'x-api-key': api_key
    }
    
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json()
    else:
        return f"Failed to retrieve data: {response.status_code} - {response.text}"

# Example usage
tracking_id = "e556fa6a-6fa7-40c7-b696-f1aa9067e711"  # Replace this with your actual tracking ID
api_key = "23426FDB-1E75-4284-AC9C-1B5804A6CF82"         # Replace this with your actual API key
result = track_package(tracking_id, api_key)
print(result)


{'trackingIdentifier': 'e556fa6a-6fa7-40c7-b696-f1aa9067e711', 'shipmentIdentifier': 'APU018257', 'identifierType': 'BL', 'transportMode': 'OCEAN', 'carrierCode': 'MAEU', 'trackingStatus': 'New', 'shipmentId': ''}