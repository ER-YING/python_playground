import requests
import json
import time
import pandas as pd
from requests_oauthlib import OAuth1
from datetime import datetime
from typing import List, Dict, Optional

class NetSuitePOBulkExtractor:
    def __init__(self, account_id: str, consumer_key: str, consumer_secret: str, 
                 token_id: str, token_secret: str, debug: bool = False):
        """
        Initialize the NetSuite Purchase Order bulk extractor for REST API v1
        """
        self.account_id = account_id
        self.api_base_url = f"https://4238542-sb1.suitetalk.api.netsuite.com"
        self.debug = debug
        
        # Convert account ID for realm (hyphen to underscore, uppercase)
        realm = account_id.replace('-', '_').upper()
        
        if self.debug:
            print(f"Account ID: 4238542-sb1")
            print(f"Realm: 4238542_SB1")
            print(f"API Base URL: {self.api_base_url}")
        
        # Create OAuth1 authentication object
        self.auth = OAuth1(
            client_key=consumer_key,
            client_secret=consumer_secret,
            resource_owner_key=token_id,
            resource_owner_secret=token_secret,
            signature_method='HMAC-SHA256',
            realm=realm,
            signature_type='auth_header'
        )
        
        self.headers = {
            "Prefer": "transient",
            "Content-Type": "application/json"
        }
        
        self.rate_limit_delay = 0.5

    def test_authentication(self):
        """Test authentication with a simple API call"""
        endpoint_url = f"https://4238542-sb1.suitetalk.api.netsuite.com/services/rest/record/v1/purchaseOrder"
        
        try:
            response = requests.get(
                endpoint_url,
                auth=self.auth,
                headers=self.headers
            )
            
            print(f"Auth Test - Status Code: {response.status_code}")
            
            if response.status_code == 200:
                print("✅ Authentication successful!")
                return True
            else:
                print(f"❌ Authentication failed: {response.status_code}")
                return False
                
        except Exception as e:
            print(f"❌ Authentication test failed: {e}")
            return False

    def get_purchase_orders_list(self, limit: int = 5, offset: int = 0) -> Dict:
        """Get a list of purchase orders with pagination"""
        endpoint_url = f"https://4238542-sb1.suitetalk.api.netsuite.com/services/rest/record/v1/purchaseOrder"
        
        params = {
            "limit": min(limit, 1000),
            "offset": offset
        }
        
        print(f"Fetching PO list - Offset: {offset}, Limit: {params['limit']}")
        
        try:
            response = requests.get(
                endpoint_url,
                auth=self.auth,
                headers=self.headers,
                params=params
            )
            
            response.raise_for_status()
            return response.json()
            
        except Exception as e:
            print(f"Error getting PO list: {e}")
            raise

    def get_single_purchase_order_from_link(self, link_href: str) -> Dict:
        """Get detailed information for a single purchase order using its self link"""
        print(f"Fetching PO details from link: {link_href}")
        
        try:
            response = requests.get(
                link_href,
                auth=self.auth,
                headers=self.headers
            )
            
            response.raise_for_status()
            po_data = response.json()
            
            print(f"✅ Successfully retrieved PO details")
            return po_data
            
        except Exception as e:
            print(f"❌ Error getting PO from link {link_href}: {e}")
            return None

    def debug_po_structure(self, po_data: Dict, po_id: str):
        """Debug function to print the actual structure"""
        print(f"\n🔍 DEBUG: Structure of PO {po_id}")
        print("="*50)
        
        # Print top-level keys
        print("Top-level fields:")
        for key in sorted(po_data.keys()):
            value = po_data[key]
            if isinstance(value, list):
                print(f"  {key}: [LIST] {len(value)} items")
                if value:
                    first_item = value[0]
                    if isinstance(first_item, dict):
                        print(f"    First item keys: {list(first_item.keys())}")
                        if len(first_item.keys()) < 10:  # Show values for small objects
                            print(f"    First item: {first_item}")
            elif isinstance(value, dict):
                print(f"  {key}: [DICT] keys: {list(value.keys())}")
                if len(value.keys()) < 5:  # Show values for small objects
                    print(f"    Value: {value}")
            else:
                print(f"  {key}: [{type(value).__name__}] = {value}")
        
        print("="*50)
        
        # Save raw structure to JSON for inspection
        with open(f"debug_po_{po_id}.json", 'w') as f:
            json.dump(po_data, f, indent=2, default=str)
        print(f"Raw PO data saved to debug_po_{po_id}.json")

    def get_detailed_purchase_orders(self, po_list: List[Dict]) -> List[Dict]:
        """Get detailed information for multiple purchase orders using their self links"""
        detailed_pos = []
        total_pos = len(po_list)
        
        for i, po_item in enumerate(po_list, 1):
            po_id = po_item.get('id', f'PO_{i}')
            print(f"Processing PO {i}/{total_pos}: {po_id}")
            
            # Find the self link
            self_link = None
            if 'links' in po_item:
                for link in po_item['links']:
                    if link.get('rel') == 'self':
                        self_link = link.get('href')
                        break
            
            if self_link:
                po_detail = self.get_single_purchase_order_from_link(self_link)
                if po_detail:
                    # Add the ID for reference
                    po_detail['list_id'] = po_id
                    detailed_pos.append(po_detail)
                    
                    # Debug the first PO structure
                    if i == 1:
                        self.debug_po_structure(po_detail, po_id)
                        
            else:
                print(f"❌ No self link found for PO {po_id}")
            
            # Rate limiting
            if i < total_pos:
                time.sleep(self.rate_limit_delay)
        
        return detailed_pos

    def extract_po_summary_safe(self, detailed_pos: List[Dict]) -> List[Dict]:
        """
        Safe extraction that handles unknown data structures
        """
        summary_data = []
        
        for i, po in enumerate(detailed_pos):
            print(f"\n🔧 Processing PO {i+1}...")
            
            # Try to find common fields with various possible names
            po_id = self.safe_get(po, ['id', 'internalId', 'internalid', 'list_id'])
            po_number = self.safe_get(po, ['tranid', 'tranId', 'documentNumber', 'number'])
            po_date = self.safe_get(po, ['trandate', 'tranDate', 'date', 'transactionDate'])
            total = self.safe_get(po, ['total', 'amount', 'totalAmount'])
            memo = self.safe_get(po, ['memo', 'description'])
            
            # Try to extract vendor information
            vendor_name = ''
            vendor_id = ''
            entity_fields = ['entity', 'vendor', 'supplier']
            for field in entity_fields:
                if field in po and po[field]:
                    entity = po[field]
                    if isinstance(entity, dict):
                        vendor_name = entity.get('refName', '') or entity.get('name', '')
                        vendor_id = entity.get('id', '')
                    else:
                        vendor_name = str(entity)
                    break
            
            # Try to extract status information
            status_name = ''
            status_id = ''
            status_fields = ['status', 'approvalStatus', 'orderStatus']
            for field in status_fields:
                if field in po and po[field]:
                    status = po[field]
                    if isinstance(status, dict):
                        status_name = status.get('refName', '') or status.get('name', '')
                        status_id = status.get('id', '')
                    else:
                        status_name = str(status)
                    break
            
            # Try to find line items
            line_items_count = 0
            item_fields = ['item', 'items', 'lineItems', 'itemList']
            for field in item_fields:
                if field in po:
                    items = po[field]
                    if isinstance(items, list):
                        line_items_count = len(items)
                        break
                    elif isinstance(items, dict) and 'item' in items:
                        items_list = items['item']
                        line_items_count = len(items_list) if isinstance(items_list, list) else 1
                        break
            
            summary = {
                'id': po_id,
                'po_number': po_number,
                'po_date': po_date,
                'vendor_id': vendor_id,
                'vendor_name': vendor_name,
                'total': self.safe_float(total),
                'status_id': status_id,
                'status_name': status_name,
                'memo': memo,
                'line_items_count': line_items_count
            }
            
            # Print what we found
            print(f"  ID: {po_id}")
            print(f"  Number: {po_number}")
            print(f"  Date: {po_date}")
            print(f"  Vendor: {vendor_name}")
            print(f"  Total: {total}")
            print(f"  Status: {status_name}")
            print(f"  Line Items: {line_items_count}")
            
            summary_data.append(summary)
        
        return summary_data

    def safe_get(self, data: Dict, keys: List[str]) -> str:
        """Safely get a value from dict using multiple possible keys"""
        for key in keys:
            if key in data and data[key] is not None:
                value = data[key]
                if isinstance(value, dict):
                    return value.get('refName', '') or value.get('name', '') or str(value)
                return str(value)
        return ''

    def safe_float(self, value) -> float:
        """Safely convert value to float"""
        if value is None:
            return 0.0
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0

    def save_to_excel(self, data: List[Dict], filename: str):
        """Save data to Excel file"""
        if data:
            df = pd.DataFrame(data)
            df.to_excel(filename, index=False)
            print(f"✅ Data saved to {filename}")
            return True
        else:
            print("❌ No data to save")
            return False

    def save_to_json(self, data: List[Dict], filename: str):
        """Save data to JSON file"""
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        print(f"✅ Data saved to {filename}")

    def print_summary(self, summary_data: List[Dict]):
        """Print a summary of the data"""
        if not summary_data:
            print("No data to summarize")
            return
        
        print("\n" + "="*80)
        print("PURCHASE ORDER SUMMARY")
        print("="*80)
        
        for i, po in enumerate(summary_data, 1):
            print(f"\n{i}. PO ID: {po.get('id', 'N/A')}")
            print(f"   PO Number: {po.get('po_number', 'N/A')}")
            print(f"   Date: {po.get('po_date', 'N/A')}")
            print(f"   Vendor: {po.get('vendor_name', 'N/A')}")
            print(f"   Total: ${po.get('total', 0):,.2f}")
            print(f"   Status: {po.get('status_name', 'N/A')}")
            print(f"   Line Items: {po.get('line_items_count', 0)}")


def main():
    # Configuration - Replace with your actual credentials
    ACCOUNT_ID = "4238542-sb1"
    CONSUMER_KEY = "528a2922fd6848a94d342c545208cff6ddbd2ffa196304d3274525987993574f"  # Your actual consumer key
    CONSUMER_SECRET = "dca1e73aab0c45a8470b5bc030626f6052c87865ca1af8ed6d8a6a317f1fa81c"  # Your actual consumer secret  
    TOKEN_ID = "8cf2de061b1b434fba38f7da67b26c66ba590616ef80a1001fbc9f1591afbe26"  # Your actual token ID
    TOKEN_SECRET = "ec8d607c25e8dece64fe185980df23cbee077d8c71425d30c97ea205ed427695"  # Your actual token secret
    
    # Check if credentials are provided
    if not all([CONSUMER_KEY, CONSUMER_SECRET, TOKEN_ID, TOKEN_SECRET]):
        print("❌ Please provide all required credentials")
        return
    
    print("🚀 NetSuite Purchase Order Extractor - CORRECTED VERSION")
    print("="*60)
    
    # Initialize the extractor
    extractor = NetSuitePOBulkExtractor(
        account_id=ACCOUNT_ID,
        consumer_key=CONSUMER_KEY,
        consumer_secret=CONSUMER_SECRET,
        token_id=TOKEN_ID,
        token_secret=TOKEN_SECRET,
        debug=True
    )
    
    try:
        # Step 1: Test authentication
        print("\n=== Step 1: Testing Authentication ===")
        if not extractor.test_authentication():
            print("❌ Authentication failed")
            return
        
        # Step 2: Get 2 purchase order items for debugging
        print("\n=== Step 2: Getting 2 Purchase Order Items ===")
        response_data = extractor.get_purchase_orders_list(limit=2, offset=0)
        
        if 'items' in response_data and response_data['items']:
            po_items = response_data['items']
            print(f"✅ Retrieved {len(po_items)} PO items")
            
            # Show what we got from the list
            for i, item in enumerate(po_items, 1):
                print(f"  Item {i}: ID = {item.get('id')}, Links = {len(item.get('links', []))}")
            
            # Step 3: Get detailed information using self links
            print("\n=== Step 3: Getting Detailed Information via Self Links ===")
            detailed_pos = extractor.get_detailed_purchase_orders(po_items)
            
            if detailed_pos:
                print(f"✅ Retrieved {len(detailed_pos)} detailed POs")
                
                # Step 4: Extract data using safe method
                print("\n=== Step 4: Processing Data with Safe Extraction ===")
                summary_data = extractor.extract_po_summary_safe(detailed_pos)
                
                # Step 5: Display results
                extractor.print_summary(summary_data)
                
                # Step 6: Save data
                print("\n=== Step 5: Saving Data ===")
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                extractor.save_to_excel(summary_data, f"po_summary_corrected_{timestamp}.xlsx")
                extractor.save_to_json(detailed_pos, f"po_raw_corrected_{timestamp}.json")
                
                print(f"\n🎉 SUCCESS! Files created:")
                print(f"   • po_summary_corrected_{timestamp}.xlsx - Processed summary")
                print(f"   • po_raw_corrected_{timestamp}.json - All raw data")
                print(f"   • debug_po_[ID].json - Raw structure of first PO")
                
            else:
                print("❌ No detailed data retrieved")
        else:
            print("❌ No purchase orders found")
    
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    try:
        import pandas
        import openpyxl
    except ImportError:
        print("❌ Please install: pip install pandas openpyxl requests requests-oauthlib")
        exit(1)
    
    main()