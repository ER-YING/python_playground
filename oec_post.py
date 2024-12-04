import requests
import json
import pandas as pd
from datetime import date
import time

def post_tracking_info(api_key):
    url = "https://api.oecgroup.ca/v1/Tracking"
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': api_key
    }
    items = []
    workbook = pd.ExcelFile('../Playwright_expercice/OEC Tracking Data.xlsx')
    trackingData= workbook.parse('Sheet1')
    trackingData = trackingData.copy()
    unique_shipments = trackingData['shipmentIdentifier'].unique()
    json_data = []
    count = 0
    success_count = 0
    fail_count = 0

    for shipment_identifier in unique_shipments:
        if shipment_identifier == "- None -":
            continue
        count += 1
        items = []
        data_for_shipment = trackingData[trackingData['shipmentIdentifier'] == shipment_identifier]
        grouped = trackingData.groupby('shipmentIdentifier')

        for index, row in data_for_shipment.iterrows():
            item = {
                "purchaseOrderNo": row['purchaseOrderNo'],
                "vendor": row['vendor'],
                "productDescription": row['productDescription'],
                "productIdentifier": row['productIdentifier'],
                "shippedQuantity": int(row['shippedQuantity'])
            }
            items.append(item)

        for name, group in grouped:
            if name != shipment_identifier:
                continue
            shipment_json = {
                "shipmentIdentifier": name,
                "identifierType": group['identifierType'].iloc[0],
                "transportMode": group['transportMode'].iloc[0].upper(),
                "carrierCode": group['carrierCode'].iloc[0],
                "consigneeName": group['consigneeName'].iloc[0],
                "pieces": int(group['pieces'].iloc[0]),
                "grossWeightKg": float(group['grossWeightKg'].iloc[0]),
                "volumeM3": float(group['volumeM3'].iloc[0]),
                "references": [],
                "items": items
            }

            shipment_json['references'].append({
                "referenceType": "ShipmentNumber",
                "referenceValue": group['ShipmentNumber'].iloc[0]
            })
            json_data.append(shipment_json)

        # for purchaseOrderNo in group['purchaseOrderNo'].unique():
        #     shipment_json['references'].append({
        #         "referenceType": "PO",
        #         "referenceValue": purchaseOrderNo
        #     })

        # print(json.dumps(json_data))
        # exit()
    print(json_data)
    print(f"Total number of shipments to be posted: {len(json_data)}")
    # exit()
    for shipment in json_data:
        response = requests.post(url, headers=headers, data=json.dumps(shipment))
        print(response)
        if response.status_code == 200:
            success_count += 1
            print(f"Successfully posted data for shipment: {shipment['shipmentIdentifier']}")
        else:
            fail_count += 1
            print(f"Failed to post data for shipment: {shipment['shipmentIdentifier']} - {response.status_code} - {response.text}")

            # Sleep for 5 seconds before making the next request
            # time.sleep(1)
            
    print(f"Total successful posts: {success_count}")
    print(f"Total failed posts: {fail_count}")
# Example usage
api_key = "23426FDB-1E75-4284-AC9C-1B5804A6CF82"  # Replace with your actual API key
result = post_tracking_info(api_key)