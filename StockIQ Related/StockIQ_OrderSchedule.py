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
    
    def fetch_order_schedule_header(self):
        """
        Fetch Order Schedule Header data from StockIQ API
        
        Returns:
            list: JSON data from API response
        """
        endpoint = "OrderScheduleHeader/"
        url = self.base_url + endpoint
        
        try:
            logger.info(f"Making API request to: {url}")
            response = requests.get(url, headers=self.headers, timeout=30)
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
            filename = f"StockIQ_OrderSchedule_{today}.csv"
        
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
    
    def run(self):
        """
        Main execution method
        """
        try:
            # Fetch data from API
            data = self.fetch_order_schedule_header()
            
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
    api_client.run()

if __name__ == "__main__":
    main()