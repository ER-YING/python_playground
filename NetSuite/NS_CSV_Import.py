import requests
import time
import hmac
import hashlib
import base64
import urllib.parse
import secrets
import json
import os

class NetSuiteOAuth:
    """OAuth 1.0 implementation for NetSuite"""
    
    def __init__(self, config):
        self.config = config
        self.account_formatted = config['ACCOUNT_ID'].lower().replace('_', '-')
        self.base_url = f"https://{self.account_formatted}.suitetalk.api.netsuite.com"
        self.realm = config['ACCOUNT_ID']
    
    def generate_oauth_header(self, url, method='GET'):
        """Generate OAuth 1.0 header"""
        
        oauth_timestamp = str(int(time.time()))
        oauth_nonce = secrets.token_hex(8)
        
        oauth_params = {
            'oauth_consumer_key': self.config['CONSUMER_KEY'],
            'oauth_token': self.config['TOKEN_ID'],
            'oauth_signature_method': 'HMAC-SHA256',
            'oauth_timestamp': oauth_timestamp,
            'oauth_nonce': oauth_nonce,
            'oauth_version': '1.0'
        }
        
        parsed_url = urllib.parse.urlparse(url)
        base_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
        query_params = urllib.parse.parse_qsl(parsed_url.query)
        
        all_params = {**oauth_params}
        for key, value in query_params:
            all_params[key] = value
        
        sorted_params = sorted(all_params.items())
        param_string = '&'.join([f"{urllib.parse.quote(str(k), safe='')}={urllib.parse.quote(str(v), safe='')}" 
                                 for k, v in sorted_params])
        
        signature_base = '&'.join([
            method.upper(),
            urllib.parse.quote(base_url, safe=''),
            urllib.parse.quote(param_string, safe='')
        ])
        
        signing_key = f"{urllib.parse.quote(self.config['CONSUMER_SECRET'], safe='')}&{urllib.parse.quote(self.config['TOKEN_SECRET'], safe='')}"
        
        signature = base64.b64encode(
            hmac.new(
                signing_key.encode('utf-8'),
                signature_base.encode('utf-8'),
                hashlib.sha256
            ).digest()
        ).decode('utf-8')
        
        encoded_signature = urllib.parse.quote(signature, safe='')
        
        auth_header = (
            f'OAuth realm="{self.realm}",'
            f'oauth_consumer_key="{oauth_params["oauth_consumer_key"]}",'
            f'oauth_token="{oauth_params["oauth_token"]}",'
            f'oauth_signature_method="{oauth_params["oauth_signature_method"]}",'
            f'oauth_timestamp="{oauth_params["oauth_timestamp"]}",'
            f'oauth_nonce="{oauth_params["oauth_nonce"]}",'
            f'oauth_version="{oauth_params["oauth_version"]}",'
            f'oauth_signature="{encoded_signature}"'
        )
        
        return auth_header
    
    def test_connection(self):
        """Test NetSuite connection"""
        print("🔍 Testing NetSuite API connection...\n")
        
        url = f"{self.base_url}/services/rest/record/v1/purchaseorder"
        params = {'limit': 1}
        full_url = f"{url}?{urllib.parse.urlencode(params)}"
        
        auth_header = self.generate_oauth_header(full_url, 'GET')
        headers = {
            'Authorization': auth_header,
            'Accept': 'application/json'
        }
        
        try:
            response = requests.get(url, params=params, headers=headers, timeout=30)
            
            if response.status_code == 200:
                print("✅ Connection successful!\n")
                return True
            else:
                print(f"❌ Connection failed: {response.status_code}")
                print(f"Response: {response.text}\n")
                return False
                
        except Exception as e:
            print(f"❌ Error: {str(e)}\n")
            return False
    
    def search_file_by_name(self, filename):
        """Search for a file in NetSuite File Cabinet by name"""
        print(f"🔍 Searching for file: {filename}...\n")
        
        url = f"{self.base_url}/services/rest/query/v1/suiteql"
        
        query = f"SELECT id, name, folder FROM file WHERE name = '{filename}' AND folder = {self.config['FILE_CABINET_FOLDER_ID']}"
        
        params = {'limit': 10}
        full_url = f"{url}?{urllib.parse.urlencode(params)}"
        
        auth_header = self.generate_oauth_header(full_url, 'POST')
        
        headers = {
            'Authorization': auth_header,
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Prefer': 'transient'
        }
        
        payload = {"q": query}
        
        try:
            response = requests.post(url, params=params, headers=headers, json=payload, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                items = data.get('items', [])
                
                if items:
                    file_id = items[0]['id']
                    print(f"✅ File found! ID: {file_id}\n")
                    return file_id
                else:
                    print(f"❌ File not found in folder {self.config['FILE_CABINET_FOLDER_ID']}\n")
                    return None
            else:
                print(f"❌ Search failed: {response.status_code}")
                print(f"Response: {response.text}\n")
                return None
                
        except Exception as e:
            print(f"❌ Error: {str(e)}\n")
            return None
    
    def trigger_csv_import(self, file_id):
        """Trigger CSV import using SuiteTalk Task API"""
        print(f"🚀 Starting CSV import with File ID: {file_id}...\n")
        
        # Use the Task API for CSV import
        url = f"{self.base_url}/services/rest/record/v1/task"
        
        auth_header = self.generate_oauth_header(url, 'POST')
        
        headers = {
            'Authorization': auth_header,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
        # Payload for CSV Import Task
        payload = {
            "taskType": "CSV_IMPORT",
            "importFile": {"id": file_id},
            "mappingId": self.config['SAVED_IMPORT_ID'],
            "queueId": 1
        }
        
        print("="*70)
        print("IMPORT JOB REQUEST (Task API)")
        print("="*70)
        print(f"URL: {url}")
        print(f"File ID: {file_id}")
        print(f"Saved Import ID: {self.config['SAVED_IMPORT_ID']}")
        print(f"Payload: {json.dumps(payload, indent=2)}")
        print("="*70 + "\n")
        
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=60)
            
            print(f"Status: {response.status_code}")
            print(f"Response: {response.text}\n")
            
            if response.status_code in [200, 201, 202]:
                print("✅ Import job submitted!\n")
                try:
                    result = response.json()
                    task_id = result.get('id')
                    if task_id:
                        print(f"📋 Task ID: {task_id}\n")
                except:
                    pass
                return True
            else:
                # Task API might not be available, try alternative approach
                print("⚠️ Task API approach failed. Trying alternative method...\n")
                return self.trigger_csv_import_alternative(file_id)
                
        except Exception as e:
            print(f"❌ Error: {str(e)}\n")
            return False
    
    def trigger_csv_import_alternative(self, file_id):
        """Alternative: Use SuiteScript scheduled script approach"""
        print(f"🔄 Attempting alternative import trigger method...\n")
        
        print("="*70)
        print("MANUAL IMPORT TRIGGER REQUIRED")
        print("="*70)
        print("Unfortunately, the REST API doesn't provide a direct CSV import trigger.")
        print("You have two options:\n")
        
        print("Option 1: Manual Import (Immediate)")
        print("-" * 70)
        print(f"1. Go to: https://{self.account_formatted}.app.netsuite.com")
        print(f"2. Navigate to: Setup > Import/Export > Import CSV Records")
        print(f"3. Click 'Load' next to your saved import (ID: {self.config['SAVED_IMPORT_ID']})")
        print(f"4. Select the uploaded file (ID: {file_id})")
        print(f"5. Click 'Next' and then 'Run'\n")
        
        print("Option 2: Use SuiteScript (Automated)")
        print("-" * 70)
        print("Create a RESTlet in NetSuite that triggers the import.")
        print("See the RESTlet code in the documentation.\n")
        
        print("="*70)
        
        response = input("\nDid you start the import manually? (y/n): ")
        
        if response.lower() == 'y':
            print("\n✅ Import process initiated manually!")
            return True
        else:
            print("\n⚠️ Import not started. File is uploaded and ready (ID: {file_id})")
            return False
    
    def manual_upload_workflow(self, csv_file_path):
        """Workflow: Manual upload + automated import trigger"""
        print("\n" + "="*70)
        print("NETSUITE CSV IMPORT - MANUAL UPLOAD WORKFLOW")
        print("="*70 + "\n")
        
        # Step 1: Test connection
        print("Step 1: Testing connection...")
        if not self.test_connection():
            print("❌ Connection failed. Stopping.\n")
            return False
        
        # Step 2: Get filename
        filename = os.path.basename(csv_file_path)
        
        # Step 3: Instructions for manual upload
        print("Step 2: Manual File Upload Required")
        print("="*70)
        print("Please upload your CSV file to NetSuite File Cabinet:")
        print(f"\n1. Login to NetSuite: https://{self.account_formatted}.app.netsuite.com")
        print(f"2. Go to: Documents > Files > File Cabinet")
        print(f"3. Navigate to folder ID: {self.config['FILE_CABINET_FOLDER_ID']}")
        print(f"4. Click 'Add File'")
        print(f"5. Upload: {csv_file_path}")
        print(f"6. Make sure the filename is: {filename}")
        print("="*70 + "\n")
        
        input("Press Enter after you've uploaded the file to NetSuite...")
        
        # Step 4: Search for the uploaded file
        print("\nStep 3: Searching for uploaded file...")
        file_id = self.search_file_by_name(filename)
        
        if not file_id:
            print("❌ File not found. Please make sure you uploaded it to the correct folder.\n")
            return False
        
        # Step 5: Trigger import
        print("Step 4: Triggering import job...")
        success = self.trigger_csv_import(file_id)
        
        if success:
            print("\n" + "="*70)
            print("🎉 PROCESS COMPLETE!")
            print("="*70)
            print(f"✅ File uploaded and found (ID: {file_id})")
            print(f"✅ Import process initiated")
            print(f"\n🔍 Monitor import status at:")
            print(f"   https://{self.account_formatted}.app.netsuite.com/app/setup/import/importstatus.nl")
            print("="*70 + "\n")
            return True
        else:
            print("\n⚠️ File uploaded successfully but import needs to be started manually.\n")
            return False


# Configuration
NETSUITE_CONFIG = {
    'ACCOUNT_ID': '4238542_SB1',
    'SAVED_IMPORT_ID': '233',
    'CONSUMER_KEY': '528a2922fd6848a94d342c545208cff6ddbd2ffa196304d3274525987993574f',
    'CONSUMER_SECRET': 'dca1e73aab0c45a8470b5bc030626f6052c87865ca1af8ed6d8a6a317f1fa81c',
    'TOKEN_ID': '8cf2de061b1b434fba38f7da67b26c66ba590616ef80a1001fbc9f1591afbe26',
    'TOKEN_SECRET': 'ec8d607c25e8dece64fe185980df23cbee077d8c71425d30c97ea205ed427695',
    'FILE_CABINET_FOLDER_ID': '16866'
}


def main():
    print("="*70)
    print("NetSuite CSV Import - Manual Upload Workflow")
    print("="*70)
    
    # Validate config
    missing = [k for k, v in NETSUITE_CONFIG.items() 
               if not v or str(v).startswith('YOUR_')]
    
    if missing:
        print("\n❌ Please update these values:")
        for key in missing:
            print(f"   - {key}")
        print()
        return
    
    netsuite = NetSuiteOAuth(NETSUITE_CONFIG)
    
    csv_file = "Test_Tradlinx_Retrieve_Shipping_Tracking_Data_20250925.csv"
    
    netsuite.manual_upload_workflow(csv_file)


if __name__ == "__main__":
    main()