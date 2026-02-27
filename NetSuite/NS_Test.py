import requests
import json
from requests_oauthlib import OAuth1
import time
from datetime import datetime
import os
import csv

# NetSuite Configuration - Replace with your actual sandbox credentials
NETSUITE_CONFIG = {
    # Your NetSuite Sandbox Account ID (e.g., "123456_SB1")
    'ACCOUNT_ID': '4238542_SB1',
    # The Internal ID of your Saved CSV Import in NetSuite
    'SAVED_IMPORT_ID': '224',
    # REAL credentials from your Sandbox Integration Record and Access Token
    'CONSUMER_KEY': '528a2922fd6848a94d342c545208cff6ddbd2ffa196304d3274525987993574f',
    'CONSUMER_SECRET': 'dca1e73aab0c45a8470b5bc030626f6052c87865ca1af8ed6d8a6a317f1fa81c',
    'TOKEN_ID': '8cf2de061b1b434fba38f7da67b26c66ba590616ef80a1001fbc9f1591afbe26',
    'TOKEN_SECRET': 'ec8d607c25e8dece64fe185980df23cbee077d8c71425d30c97ea205ed427695',
    # The internal ID of the NetSuite File Cabinet folder for the upload.
    # Folder -15 is the "SuiteScripts" folder and is a safe default.
    'FILE_CABINET_FOLDER_ID': '16866'
}



class NetSuiteIntegration:
    def __init__(self, config):
        self.config = config
        self.account_formatted = config['ACCOUNT_ID'].lower().replace('_', '-')
        self.base_url = f"https://{self.account_formatted}.suitetalk.api.netsuite.com"
        self.realm = config['ACCOUNT_ID'].replace('_', '-').upper()
        
    def create_oauth_auth(self, url, method='GET'):
        """Create OAuth1 authentication object"""
        return OAuth1(
            client_key=self.config['CONSUMER_KEY'],
            client_secret=self.config['CONSUMER_SECRET'],
            resource_owner_key=self.config['TOKEN_ID'],
            resource_owner_secret=self.config['TOKEN_SECRET'],
            realm=self.realm,
            signature_method='HMAC-SHA256',
            signature_type='AUTH_HEADER'
        )
    
    def test_connection(self):
        """Test NetSuite API connection"""
        print("🔍 Testing NetSuite API connection...")
        
        url = f"{self.base_url}/services/rest/record/v1/item"
        
        try:
            auth = self.create_oauth_auth(url, 'GET')
            
            response = requests.get(
                url,
                auth=auth,
                headers={'Accept': 'application/json'},
                params={'limit': 1},
                timeout=30
            )
            
            if response.status_code == 200:
                print("✅ NetSuite API connection successful!")
                return True
            elif response.status_code == 401:
                print("❌ Authentication failed. Please check your OAuth credentials.")
                print(f"Response: {response.text}")
                return False
            elif response.status_code == 403:
                print("❌ Access forbidden. Please check your role permissions.")
                print(f"Response: {response.text}")
                return False
            else:
                print(f"❌ Unexpected response: {response.status_code}")
                print(f"Response: {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Connection error: {str(e)}")
            return False
    
    def validate_csv_file(self, csv_file_path):
        """Validate CSV file exists and has data"""
        if not os.path.exists(csv_file_path):
            print(f"❌ CSV file not found: {csv_file_path}")
            return False
        
        try:
            with open(csv_file_path, 'r', encoding='utf-8') as file:
                reader = csv.reader(file)
                rows = list(reader)
                
            if len(rows) < 2:  # Header + at least 1 data row
                print(f"❌ CSV file has no data rows")
                return False
                
            print(f"✅ CSV file validated: {len(rows)-1} data rows found")
            print(f"Headers: {rows[0] if rows else 'None'}")
            return True
            
        except Exception as e:
            print(f"❌ Error reading CSV file: {str(e)}")
            return False
    
    def upload_file_to_netsuite(self, csv_file_path):
        """Upload CSV file to NetSuite File Cabinet"""
        print(f"📤 Uploading file to NetSuite: {csv_file_path}")
        
        try:
            # Read file content
            with open(csv_file_path, 'r', encoding='utf-8') as file:
                file_content = file.read()
            
            filename = os.path.basename(csv_file_path)
            
            # NetSuite file upload endpoint
            url = f"{self.base_url}/services/rest/file/v1/files"
            
            auth = self.create_oauth_auth(url, 'POST')
            
            # Prepare multipart form data
            files = {
                'file': (filename, file_content, 'text/csv')
            }
            
            data = {
                'folder': self.config['FILE_CABINET_FOLDER_ID']
            }
            
            response = requests.post(
                url,
                auth=auth,
                files=files,
                data=data,
                timeout=120
            )
            
            print(f"Upload response status: {response.status_code}")
            
            if response.status_code in [200, 201]:
                print("✅ File uploaded successfully!")
                
                try:
                    response_json = response.json()
                    file_id = response_json.get('id')
                    
                    if file_id:
                        print(f"📁 NetSuite File ID: {file_id}")
                        return file_id
                    else:
                        print("⚠️ File uploaded but couldn't get File ID from response")
                        print(f"Response: {response.text}")
                        return None
                        
                except json.JSONDecodeError:
                    print("⚠️ File uploaded but response is not JSON")
                    print(f"Response: {response.text}")
                    return None
            else:
                print("❌ File upload failed!")
                print(f"Response: {response.text}")
                return None
                
        except Exception as e:
            print(f"❌ File upload error: {str(e)}")
            return None
    
    def start_import_job(self, file_id):
        """Start NetSuite CSV Import job"""
        print(f"🚀 Starting import job with File ID: {file_id}")
        
        try:
            url = f"{self.base_url}/services/rest/async/job/v1/import/job"
            
            payload = {
                "jobName": f"Tradlinx_Import_{int(time.time())}",
                "importDefinition": {
                    "savedImport": {"id": self.config['SAVED_IMPORT_ID']}
                },
                "fileReference": {
                    "id": file_id
                }
            }
            
            auth = self.create_oauth_auth(url, 'POST')
            
            response = requests.post(
                url,
                auth=auth,
                json=payload,
                headers={
                    'Content-Type': 'application/vnd.oracle.adf.action+json',
                    'Prefer': 'respond-async',
                    'Accept': 'application/json'
                },
                timeout=60
            )
            
            print(f"Import job response status: {response.status_code}")
            
            if response.status_code == 202:
                print("✅ Import job started successfully!")
                
                # Try to get job ID from Location header
                location_header = response.headers.get('Location', '')
                if location_header:
                    job_id = location_header.split('/')[-1]
                    print(f"📋 Job ID: {job_id}")
                    return job_id
                else:
                    print("⚠️ Job started but couldn't get Job ID from Location header")
                    print(f"Location header: {location_header}")
                    return True  # Job started, but no ID
                    
            else:
                print("❌ Failed to start import job!")
                print(f"Response: {response.text}")
                return None
                
        except Exception as e:
            print(f"❌ Import job error: {str(e)}")
            return None
    
    def get_import_status_url(self):
        """Get NetSuite import status monitoring URL"""
        return f"https://{self.account_formatted}.app.netsuite.com/app/setup/import/importstatus.nl"
    
    def process_csv_to_netsuite(self, csv_file_path):
        """Complete workflow: validate, upload, and import CSV to NetSuite"""
        print("=== Starting NetSuite CSV Import Process ===\n")
        
        # Step 1: Test connection
        if not self.test_connection():
            print("❌ Connection test failed. Aborting process.")
            return False
        
        # Step 2: Validate CSV file
        if not self.validate_csv_file(csv_file_path):
            print("❌ CSV validation failed. Aborting process.")
            return False
        
        # Step 3: Upload file
        file_id = self.upload_file_to_netsuite(csv_file_path)
        if not file_id:
            print("❌ File upload failed. Aborting process.")
            return False
        
        # Step 4: Start import job
        job_result = self.start_import_job(file_id)
        if not job_result:
            print("❌ Import job failed to start. Aborting process.")
            return False
        
        # Step 5: Success summary
        print("\n" + "="*60)
        print("🎉 SUCCESS! CSV Import Process Completed")
        print("="*60)
        print(f"✅ File uploaded to NetSuite File Cabinet")
        print(f"✅ Import job started successfully")
        print(f"📁 File ID: {file_id}")
        
        if isinstance(job_result, str):
            print(f"📋 Job ID: {job_result}")
        
        print(f"\n🔍 Monitor import progress at:")
        print(f"   {self.get_import_status_url()}")
        print("\n💡 The import will process in the background.")
        print("   Check the Import Status page in NetSuite for results.")
        
        return True

def main():
    """Main function to run the complete NetSuite integration"""
    print("=== NetSuite CSV Import Tool ===\n")
    
    # Configuration validation
    missing_config = []
    for key, value in NETSUITE_CONFIG.items():
        if not value or str(value).startswith('YOUR_'):
            missing_config.append(key)
    
    if missing_config:
        print("❌ Missing configuration values:")
        for config in missing_config:
            print(f"   - {config}")
        print("\nPlease update NETSUITE_CONFIG with your actual NetSuite credentials.")
        return
    
    # CSV file path
    csv_file_path = "Tradlinx_Retrieve_Shipping_Tracking_Data_20250925.csv"
    
    # Initialize NetSuite integration
    netsuite = NetSuiteIntegration(NETSUITE_CONFIG)
    
    # Process the CSV
    success = netsuite.process_csv_to_netsuite(csv_file_path)
    
    if success:
        print("\n🎊 All done! Check NetSuite for your imported data.")
    else:
        print("\n💔 Process failed. Please check the errors above and try again.")

if __name__ == "__main__":
    # Install required packages if needed
    try:
        import requests_oauthlib
    except ImportError:
        print("Installing required packages...")
        import subprocess
        subprocess.check_call(['pip', 'install', 'requests', 'requests-oauthlib'])
    
    main()