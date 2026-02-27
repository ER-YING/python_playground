import requests
import time
import hmac
import hashlib
import base64
import urllib.parse
import secrets
import json
import os

class NetSuiteAutomatedImport:
    """Fully automated NetSuite CSV import using RESTlet"""
    
    def __init__(self, config):
        self.config = config
        self.account_formatted = config['ACCOUNT_ID'].lower().replace('_', '-')
        self.realm = config['ACCOUNT_ID']
        self.restlet_url = config['RESTLET_URL']
    
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
    
    def test_restlet_connection(self):
        """Test RESTlet connection"""
        print("🔍 Testing RESTlet connection...\n")
        
        auth_header = self.generate_oauth_header(self.restlet_url, 'GET')
        
        headers = {
            'Authorization': auth_header,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
        try:
            response = requests.get(self.restlet_url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                print("✅ RESTlet connection successful!")
                print(f"   Message: {data.get('message', '')}")
                print(f"   Version: {data.get('version', '')}")
                print(f"   User: {data.get('user', '')}")
                print(f"   Account: {data.get('account', '')}\n")
                return True
            else:
                print(f"❌ RESTlet connection failed: {response.status_code}")
                print(f"   Response: {response.text}\n")
                return False
                
        except Exception as e:
            print(f"❌ Connection error: {str(e)}\n")
            return False
    
    def upload_and_import_csv(self, csv_file_path):
        """Upload CSV and trigger import in one call"""
        print(f"📤 Uploading and importing: {csv_file_path}...\n")
        
        try:
            with open(csv_file_path, 'r', encoding='utf-8') as f:
                file_content = f.read()
            
            filename = os.path.basename(csv_file_path)
            
            payload = {
                "filename": filename,
                "content": file_content,
                "folderId": self.config['FILE_CABINET_FOLDER_ID'],
                "savedImportId": self.config['SAVED_IMPORT_ID']
            }
            
            auth_header = self.generate_oauth_header(self.restlet_url, 'POST')
            
            headers = {
                'Authorization': auth_header,
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
            
            print("="*70)
            print("UPLOAD & IMPORT REQUEST")
            print("="*70)
            print(f"Filename: {filename}")
            print(f"File size: {len(file_content)} bytes")
            print(f"Folder ID: {self.config['FILE_CABINET_FOLDER_ID']}")
            print(f"Saved Import ID: {self.config['SAVED_IMPORT_ID']}")
            print("="*70 + "\n")
            
            response = requests.post(
                self.restlet_url,
                headers=headers,
                json=payload,
                timeout=120
            )
            
            print(f"Status: {response.status_code}")
            
            if response.status_code == 200:
                result = response.json()
                print(f"\nResponse:")
                print(json.dumps(result, indent=2))
                print()
                
                if result.get('success'):
                    print("✅ SUCCESS!")
                    print(f"   📁 File ID: {result.get('fileId')}")
                    
                    if result.get('importTaskId'):
                        print(f"   📋 Import Task ID: {result.get('importTaskId')}")
                        print(f"   ✅ Import started automatically")
                    elif result.get('warning'):
                        print(f"   ⚠️  Warning: {result.get('warning')}")
                    
                    return {
                        'success': True,
                        'fileId': result.get('fileId'),
                        'importTaskId': result.get('importTaskId')
                    }
                else:
                    print(f"❌ Failed: {result.get('error')}")
                    if result.get('details'):
                        print(f"   Details: {result.get('details')}")
                    return {'success': False, 'error': result.get('error')}
            else:
                print(f"❌ Request failed")
                print(f"Response: {response.text}\n")
                return {'success': False, 'error': response.text}
                
        except Exception as e:
            print(f"❌ Error: {str(e)}\n")
            import traceback
            traceback.print_exc()
            return {'success': False, 'error': str(e)}
    
    def run(self, csv_file_path):
        """Main workflow"""
        print("\n" + "="*70)
        print("NETSUITE FULLY AUTOMATED CSV IMPORT")
        print("="*70 + "\n")
        
        # Validate CSV file
        if not os.path.exists(csv_file_path):
            print(f"❌ CSV file not found: {csv_file_path}\n")
            return False
        
        file_size = os.path.getsize(csv_file_path)
        print(f"✅ CSV file found: {csv_file_path} ({file_size} bytes)\n")
        
        # Test RESTlet
        print("Step 1: Testing RESTlet connection...")
        if not self.test_restlet_connection():
            print("❌ RESTlet connection failed.\n")
            print("Troubleshooting:")
            print("1. Verify RESTlet is deployed with Status = RELEASED")
            print("2. Check that RESTLET_URL is correct")
            print("3. Verify your role is in the Audience list\n")
            return False
        
        # Upload and import
        print("Step 2: Uploading file and triggering import...")
        result = self.upload_and_import_csv(csv_file_path)
        
        if result.get('success'):
            print("\n" + "="*70)
            print("🎉 FULLY AUTOMATED PROCESS COMPLETE!")
            print("="*70)
            print(f"✅ File uploaded automatically")
            print(f"✅ Import job triggered automatically")
            
            if result.get('importTaskId'):
                print(f"\n📋 Import Task ID: {result.get('importTaskId')}")
            
            print(f"\n🔍 Monitor import status at:")
            print(f"   https://{self.account_formatted}.app.netsuite.com/app/setup/import/importstatus.nl")
            print("="*70 + "\n")
            return True
        else:
            print("\n❌ Process failed.\n")
            return False


# ========== CONFIGURATION ==========
NETSUITE_CONFIG = {
    'ACCOUNT_ID': '4238542_SB1',
    'SAVED_IMPORT_ID': '224',
    'CONSUMER_KEY': '528a2922fd6848a94d342c545208cff6ddbd2ffa196304d3274525987993574f',
    'CONSUMER_SECRET': 'dca1e73aab0c45a8470b5bc030626f6052c87865ca1af8ed6d8a6a317f1fa81c',
    'TOKEN_ID': '8cf2de061b1b434fba38f7da67b26c66ba590616ef80a1001fbc9f1591afbe26',
    'TOKEN_SECRET': 'ec8d607c25e8dece64fe185980df23cbee077d8c71425d30c97ea205ed427695',
    'FILE_CABINET_FOLDER_ID': '16866',
    'RESTLET_URL': 'https://4238542-sb1.restlets.api.netsuite.com/app/site/hosting/restlet.nl?script=1985&deploy=1'  # Get this after deploying the RESTlet
}


def main():
    """Main entry point"""
    print("="*70)
    print("NetSuite Fully Automated CSV Import")
    print("="*70)
    
    # Validate configuration
    missing = []
    for key, value in NETSUITE_CONFIG.items():
        if not value or str(value).startswith('YOUR_'):
            missing.append(key)
    
    if missing:
        print("\n❌ Please update these configuration values:")
        for key in missing:
            print(f"   - {key}")
        print("\nConfiguration instructions:")
        print("1. CONSUMER_SECRET & TOKEN_SECRET: From NetSuite credentials")
        print("2. RESTLET_URL: From NetSuite after deploying the RESTlet")
        print("   Example: https://4238542-sb1.restlets.api.netsuite.com/app/site/hosting/restlet.nl?script=123&deploy=1")
        print()
        return
    
    # CSV file path
    csv_file_path = "Test_Tradlinx_Retrieve_Shipping_Tracking_Data_20251105.csv"
    
    # Run the automation
    netsuite = NetSuiteAutomatedImport(NETSUITE_CONFIG)
    netsuite.run(csv_file_path)


if __name__ == "__main__":
    main()