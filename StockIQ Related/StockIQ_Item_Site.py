import requests
import csv
import json
import logging
from datetime import datetime
from pathlib import Path

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StockIQAPI:
    def __init__(self):
        self.base_url = "https://earthrated.stockiqtech.net/api/"
        self.headers = {
            'Authorization': 'Basic WWluZzpLeDUmZn1IM0Y4',
            'Content-Type': 'application/json'
        }
    
    def fetch_item_site_attributes(self, site_code=None, item_code=None):
        """
        Fetch Item Site Attributes data from StockIQ API
        
        Args:
            site_code (str): Optional site code to filter by specific site
            item_code (str): Optional item code (must be used with site_code)
        
        Returns:
            list: JSON data from API response
        """
        endpoint = "ItemSiteAttributes/"
        url = self.base_url + endpoint
        
        # Build query parameters
        params = {}
        if site_code:
            params['siteCode'] = site_code
        if item_code:
            if not site_code:
                logger.warning("item_code specified without site_code. site_code is required when using item_code.")
                raise ValueError("site_code must be specified when using item_code")
            params['itemCode'] = item_code
        
        try:
            logger.info(f"Making API request to: {url}")
            if params:
                logger.info(f"With parameters: {params}")
            
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Successfully fetched {len(data)} records from API")
            return data
            
        except requests.exceptions.Timeout:
            logger.error("API request timed out")
            raise
        except requests.exceptions.ConnectionError:
            logger.error("Connection error when accessing API")
            raise
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error: {e}")
            logger.error(f"Response content: {response.text}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON response: {e}")
            raise
    
    def save_to_csv(self, data, filename=None):
        """
        Save data to CSV file
        
        Args:
            data (list): Data to save
            filename (str): Optional filename, defaults to date-based name
        """
        if not data:
            logger.warning("No data to save")
            return
        
        if filename is None:
            today = datetime.now().strftime("%Y-%m-%d")
            filename = f"StockIQ_ItemSiteAttributes_{today}.csv"
        
        # Ensure we have a valid path
        filepath = Path(filename)
        
        # Get all unique keys for CSV headers
        all_keys = set()
        for item in data:
            if isinstance(item, dict):
                all_keys.update(item.keys())
        
        fieldnames = sorted(all_keys)
        
        try:
            logger.info(f"Saving {len(data)} records to {filepath}")
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(data)
            
            logger.info(f"Successfully saved data to {filepath}")
            logger.info(f"File size: {filepath.stat().st_size} bytes")
            logger.info(f"Columns: {len(fieldnames)}")
            
        except IOError as e:
            logger.error(f"Error writing to file {filepath}: {e}")
            raise
    
    def run(self, site_code=None, item_code=None):
        """
        Main execution method
        
        Args:
            site_code (str): Optional site code to filter results
            item_code (str): Optional item code to filter results (requires site_code)
        """
        try:
            # Fetch data from API
            data = self.fetch_item_site_attributes(site_code=site_code, item_code=item_code)
            
            # Save to CSV
            self.save_to_csv(data)
            
            logger.info("Process completed successfully")
            
        except Exception as e:
            logger.error(f"Process failed: {e}")
            raise

def main():
    """
    Entry point for the script
    """
    api_client = StockIQAPI()
    
    # Option 1: Get all item-site attributes (no filters)
    api_client.run()
    
    # Option 2: Get attributes for a specific site only
    # api_client.run(site_code="YOUR_SITE_CODE")
    
    # Option 3: Get attributes for a specific item at a specific site
    # api_client.run(site_code="YOUR_SITE_CODE", item_code="YOUR_ITEM_CODE")

if __name__ == "__main__":
    main()