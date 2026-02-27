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
    def __init__(self, base_url=None, auth_token=None):
        """
        Initialize StockIQ API client
        
        Args:
            base_url (str): Base URL for the API (optional, uses default if not provided)
            auth_token (str): Basic auth token (optional, uses default if not provided)
        """
        self.base_url = base_url or "https://earthrated.stockiqtech.net/api/"
        self.headers = {
            'Authorization': f'Basic {auth_token or "WWluZzpLeDUmZn1IM0Y4"}',
            'Content-Type': 'application/json'
        }
        
        # Time interval mapping
        self.INTERVALS = {
            'weekly': 2,
            'monthly': 4,
            'quarterly': 8,
            'yearly': 16
        }
    
    def fetch_item_site_inventory_snapshot(
        self, 
        time_interval='monthly',
        show_current_values=True,
        show_historical_values=True,
        show_projected_values=False,
        timeout=60
    ):
        """
        Fetch Item Site Inventory Snapshot data from StockIQ API
        
        Args:
            time_interval (str or int): Time interval for the data
                - Can be str: 'weekly', 'monthly', 'quarterly', 'yearly'
                - Can be int: 2 (Weekly), 4 (Monthly), 8 (Quarterly), 16 (Yearly)
            show_current_values (bool): True to show current day snapshot
            show_historical_values (bool): True to show historical values
            show_projected_values (bool): True to show projected values
            timeout (int): Request timeout in seconds (default 60 for larger datasets)
        
        Returns:
            dict or list: JSON data from API response
        """
        endpoint = "ItemSiteInventorySnapshot/"
        url = self.base_url + endpoint
        
        # Convert string interval to int if necessary
        if isinstance(time_interval, str):
            interval_lower = time_interval.lower()
            if interval_lower not in self.INTERVALS:
                raise ValueError(f"Invalid interval string. Must be one of: {list(self.INTERVALS.keys())}")
            interval_value = self.INTERVALS[interval_lower]
        elif isinstance(time_interval, int):
            if time_interval not in [2, 4, 8, 16]:
                raise ValueError("Invalid interval value. Must be 2 (Weekly), 4 (Monthly), 8 (Quarterly), or 16 (Yearly)")
            interval_value = time_interval
        else:
            raise ValueError("time_interval must be either an int or string")
        
        # Build query parameters
        params = {
            'timeInterval': interval_value,
            'showCurrentValues': str(show_current_values).lower(),
            'showHistoricalValues': str(show_historical_values).lower(),
            'showProjectedValues': str(show_projected_values).lower()
        }
        
        # Get interval name for logging
        interval_name = {2: 'Weekly', 4: 'Monthly', 8: 'Quarterly', 16: 'Yearly'}.get(interval_value)
        
        try:
            logger.info(f"Making API request to: {url}")
            logger.info(f"Time Interval: {interval_name} ({interval_value})")
            logger.info(f"Show Current Values: {show_current_values}")
            logger.info(f"Show Historical Values: {show_historical_values}")
            logger.info(f"Show Projected Values: {show_projected_values}")
            
            response = requests.get(url, headers=self.headers, params=params, timeout=timeout)
            response.raise_for_status()
            
            data = response.json()
            
            # Log the structure for debugging
            logger.info(f"Response type: {type(data)}")
            
            if isinstance(data, dict):
                logger.info(f"Response is a dictionary with keys: {list(data.keys())}")
                # Try to find the actual data array
                for key, value in data.items():
                    if isinstance(value, list):
                        logger.info(f"Found list under key '{key}' with {len(value)} items")
            elif isinstance(data, list):
                logger.info(f"Response is a list with {len(data)} items")
                if data and len(data) > 0:
                    logger.info(f"First item type: {type(data[0])}")
            
            return data
            
        except requests.exceptions.Timeout:
            logger.error(f"API request timed out after {timeout} seconds")
            logger.error("Try reducing the dataset or increasing timeout")
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
    
    def extract_data_array(self, data):
        """
        Extract the actual data array from the response
        
        Args:
            data (dict or list): Raw API response
            
        Returns:
            list: Extracted data array
        """
        # If it's already a list, return it
        if isinstance(data, list):
            return data
        
        # If it's a dict, try to find the data array
        if isinstance(data, dict):
            # Common keys that might contain the data
            possible_keys = ['data', 'items', 'records', 'results', 'ItemSiteHistory']
            
            for key in possible_keys:
                if key in data and isinstance(data[key], list):
                    logger.info(f"Extracting data from key: {key}")
                    return data[key]
            
            # If no standard key found, look for the first list value
            for key, value in data.items():
                if isinstance(value, list):
                    logger.info(f"Extracting data from key: {key}")
                    return value
            
            # If still no list found, return the dict as a single-item list
            logger.warning("No list found in response, treating entire response as single record")
            return [data]
        
        # Fallback: wrap in list
        return [data]
    
    def flatten_data(self, data):
        """
        Flatten nested data structures for CSV export
        
        Args:
            data (list): Raw data from API
            
        Returns:
            list: Flattened list of dictionaries
        """
        flattened = []
        
        for item in data:
            if isinstance(item, dict):
                flat_item = {}
                for key, value in item.items():
                    if isinstance(value, (dict, list)):
                        # Convert nested structures to JSON strings
                        flat_item[key] = json.dumps(value)
                    else:
                        flat_item[key] = value
                flattened.append(flat_item)
            elif isinstance(item, str):
                # If item is a string, try to parse it as JSON
                try:
                    parsed = json.loads(item)
                    if isinstance(parsed, dict):
                        flattened.append(parsed)
                    else:
                        flattened.append({'value': item})
                except json.JSONDecodeError:
                    flattened.append({'value': item})
            else:
                # For other types, create a simple dict
                flattened.append({'value': str(item)})
        
        return flattened
    
    def save_to_csv(self, data, filename=None, time_interval=None):
        """
        Save data to CSV file
        
        Args:
            data (dict or list): Data to save
            filename (str): Optional filename, defaults to date-based name
            time_interval (int): Optional interval value for filename
        """
        if not data:
            logger.warning("No data to save")
            return
        
        if filename is None:
            today = datetime.now().strftime("%Y-%m-%d")
            interval_name = ""
            if time_interval:
                interval_map = {2: 'Weekly', 4: 'Monthly', 8: 'Quarterly', 16: 'Yearly'}
                interval_name = f"_{interval_map.get(time_interval, '')}"
            filename = f"StockIQ_InventorySnapshot{interval_name}_{today}.csv"
        
        # Ensure we have a valid path
        filepath = Path(filename)
        
        # Extract data array from response
        data_array = self.extract_data_array(data)
        
        # Flatten the data to handle nested structures
        flattened_data = self.flatten_data(data_array)
        
        if not flattened_data:
            logger.warning("No data to save after flattening")
            return
        
        # Get all unique keys for CSV headers
        all_keys = set()
        for item in flattened_data:
            if isinstance(item, dict):
                all_keys.update(item.keys())
        
        fieldnames = sorted(all_keys)
        
        try:
            logger.info(f"Saving {len(flattened_data)} records to {filepath}")
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(flattened_data)
            
            logger.info(f"Successfully saved data to {filepath}")
            logger.info(f"File size: {filepath.stat().st_size} bytes")
            logger.info(f"Columns: {len(fieldnames)}")
            
        except IOError as e:
            logger.error(f"Error writing to file {filepath}: {e}")
            raise
        except Exception as e:
            logger.error(f"Error processing data: {e}")
            logger.error(f"Data sample: {flattened_data[:2] if len(flattened_data) > 0 else 'empty'}")
            raise
    
    def save_to_json(self, data, filename=None):
        """
        Save data to JSON file
        
        Args:
            data (dict or list): Data to save
            filename (str): Optional filename
        """
        if not data:
            logger.warning("No data to save")
            return
        
        if filename is None:
            today = datetime.now().strftime("%Y-%m-%d")
            filename = f"StockIQ_InventorySnapshot_{today}.json"
        
        filepath = Path(filename)
        
        try:
            logger.info(f"Saving data to {filepath}")
            with open(filepath, 'w', encoding='utf-8') as jsonfile:
                json.dump(data, jsonfile, indent=2, ensure_ascii=False)
            
            logger.info(f"Successfully saved data to {filepath}")
            logger.info(f"File size: {filepath.stat().st_size} bytes")
            
        except IOError as e:
            logger.error(f"Error writing to file {filepath}: {e}")
            raise
    
    def run_inventory_snapshot(
        self,
        time_interval='monthly',
        show_current_values=True,
        show_historical_values=True,
        show_projected_values=False,
        output_format='csv',
        filename=None
    ):
        """
        Main execution method for Inventory Snapshot
        
        Args:
            time_interval (str or int): Time interval ('weekly', 'monthly', 'quarterly', 'yearly' or 2, 4, 8, 16)
            show_current_values (bool): Include current day snapshot
            show_historical_values (bool): Include historical values
            show_projected_values (bool): Include projected values
            output_format (str): 'csv' or 'json'
            filename (str): Optional custom filename
        """
        try:
            # Convert string to int for filename
            if isinstance(time_interval, str):
                interval_value = self.INTERVALS.get(time_interval.lower())
            else:
                interval_value = time_interval
            
            # Fetch data from API
            data = self.fetch_item_site_inventory_snapshot(
                time_interval=time_interval,
                show_current_values=show_current_values,
                show_historical_values=show_historical_values,
                show_projected_values=show_projected_values
            )
            
            # Save to file
            if output_format.lower() == 'json':
                self.save_to_json(data, filename=filename)
            else:
                self.save_to_csv(data, filename=filename, time_interval=interval_value)
            
            logger.info("Process completed successfully")
            return data
            
        except Exception as e:
            logger.error(f"Process failed: {e}")
            raise


def main():
    """
    Entry point for the script - Get current month's inventory snapshot
    """
    api_client = StockIQAPI()
    
    # Get current month's data
    # Save as both JSON and CSV to see the structure
    logger.info("=" * 80)
    logger.info("Saving as JSON first to inspect structure...")
    logger.info("=" * 80)
    api_client.run_inventory_snapshot(
        time_interval='monthly',
        show_current_values=True,
        show_historical_values=True,
        show_projected_values=True,
        output_format='json'
    )
    
    logger.info("=" * 80)
    logger.info("Now saving as CSV...")
    logger.info("=" * 80)
    api_client.run_inventory_snapshot(
        time_interval='monthly',
        show_current_values=True,
        show_historical_values=True,
        show_projected_values=True,
        output_format='csv'
    )


if __name__ == "__main__":
    main()