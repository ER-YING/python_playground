import requests
import pandas as pd
from datetime import datetime

def get_all_stockiq_data():
    """
    Simple function to retrieve ALL data and ALL columns from StockIQ API
    """
    # API configuration
    url = "https://earthrated.stockiqtech.net/api/BottomLevelForecastDetail"
    headers = {
        'Authorization': 'Basic WWluZzpLeDUmZn1IM0Y4',
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    params = {
        'interval': 4,
        'demandForecastSeriesId': 1
    }
    
    try:
        print("🚀 Retrieving data from StockIQ...")
        
        # Make API call
        response = requests.get(url, headers=headers, params=params, timeout=60)
        
        if response.status_code == 200:
            print("✅ API call successful!")
            
            # Parse JSON response
            data = response.json()
            
            # Handle different response structures
            if isinstance(data, dict):
                if 'results' in data:
                    records = data['results']
                elif 'data' in data:
                    records = data['data']
                else:
                    # Find first list in the response
                    for key, value in data.items():
                        if isinstance(value, list):
                            records = value
                            break
                    else:
                        records = [data]
            elif isinstance(data, list):
                records = data
            else:
                print(f"❌ Unexpected data format")
                return None
            
            # Convert to DataFrame (this keeps ALL columns)
            df = pd.DataFrame(records)
            
            print(f"📊 Retrieved {len(df):,} records with {len(df.columns)} columns")
            
            # Display all column names
            print(f"\n📋 Available columns ({len(df.columns)}):")
            for i, col in enumerate(sorted(df.columns), 1):
                print(f"  {i}. {col}")
            
            # Save to CSV with timestamp
            timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
            filename = f"StockIQ_All_Data_{timestamp}.csv"
            df.to_csv(filename, index=False)
            
            print(f"\n💾 Data saved to: {filename}")
            print(f"📊 Total records: {len(df):,}")
            print(f"📊 Total columns: {len(df.columns)}")
            
            # Show preview
            print(f"\n👀 First 5 rows preview:")
            print(df.head(5))
            
            return df
            
        else:
            print(f"❌ API call failed with status code: {response.status_code}")
            print(f"Response: {response.text[:500]}")
            return None
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return None


if __name__ == "__main__":
    print("=" * 80)
    print("🚀 StockIQ - Retrieve All Data")
    print("=" * 80)
    
    result = get_all_stockiq_data()
    
    if result is not None:
        print("\n✅ Process completed successfully!")
    else:
        print("\n❌ Process failed!")