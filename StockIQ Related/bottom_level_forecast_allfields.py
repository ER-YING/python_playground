
import requests
import pandas as pd
from datetime import date, datetime
import json

def get_bottom_level_forecast_data():
    # API configuration
    url = "https://earthrated.stockiqtech.net/api/BottomLevelForecastDetail"
    headers = {
        "Authorization": "Basic WWluZzpLeDUmZn1IM0Y4",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    params = {
        "interval": 4,
        "demandForecastSeriesId": 1,
    }

    # Filters (use StockIQ field names directly)
    start_date = date(2024, 7, 1)  # column 'A' >= this date
    abc_filter = ["A", "B", "C", "D", "I", "N/A", "Unassigned"]  # column 'ISC4'
    product_category_filter = [  # column 'ISC1'
        "Cleanup : Bags",
        "Cleanup : Dispensers",
        "Grooming : Wipes",
        "Grooming : Specialty Wipes",
        "Grooming : Waterless",
        "Grooming : Shampoo",
        "Toys : Interactive Toys",
        "Toys : Standalone",
    ]
    site_code_filter = [  # column 'S'
        "Source NJ",
        "Northland Goreway",
        "Source Montebello",
        "Progressive UK",
        "Rhenus Netherlands",
        "Golden State FC LLC (AGL)",
    ]

    try:
        print("Making API call to StockIQ Bottom Level Forecast...")
        response = requests.get(url, headers=headers, params=params, timeout=60)
        print(f"Status: {response.status_code}")

        if response.status_code != 200:
            print("API call failed.")
            print(f"Response snippet: {response.text[:500]}")
            return None

        # Parse JSON
        try:
            data = response.json()
        except json.JSONDecodeError as e:
            print(f"Failed to parse JSON: {e}")
            return None

        # Normalize to list of records
        if isinstance(data, dict):
            if "results" in data:
                records = data["results"]
            elif "data" in data:
                records = data["data"]
            else:
                records = None
                for _, v in data.items():
                    if isinstance(v, list):
                        records = v
                        break
                if records is None:
                    records = [data]
        elif isinstance(data, list):
            records = data
        else:
            print(f"Unexpected data format: {type(data)}")
            return None

        if not records:
            print("No records found in response.")
            return None

        df = pd.DataFrame(records)
        print(f"Total records: {len(df):,}")
        print(f"Columns ({len(df.columns)}): {list(df.columns)}")

        # Apply filters (keep ALL columns; no renaming)
        df_filtered = df.copy()

        # Item code starts with '10' (column 'I')
        if "I" in df_filtered.columns:
            df_filtered = df_filtered[df_filtered["I"].astype(str).str.startswith("10")]
            print(f"After Item filter (I startswith '10'): {len(df_filtered):,}")
        else:
            print("Skipping Item filter: 'I' not found")

        # NetSuite ABC (column 'ISC4')
        if "ISC4" in df_filtered.columns:
            df_filtered = df_filtered[df_filtered["ISC4"].isin(abc_filter)]
            print(f"After NetSuite ABC filter (ISC4): {len(df_filtered):,}")
        else:
            print("Skipping ABC filter: 'ISC4' not found")

        # Product Category (column 'ISC1')
        if "ISC1" in df_filtered.columns:
            df_filtered = df_filtered[df_filtered["ISC1"].isin(product_category_filter)]
            print(f"After Product Category filter (ISC1): {len(df_filtered):,}")
        else:
            print("Skipping Product Category filter: 'ISC1' not found")

        # Site Code (column 'S')
        if "S" in df_filtered.columns:
            df_filtered = df_filtered[df_filtered["S"].isin(site_code_filter)]
            print(f"After Site filter (S): {len(df_filtered):,}")
        else:
            print("Skipping Site filter: 'S' not found")

        # Date filter (column 'A')
        if "A" in df_filtered.columns:
            df_filtered = df_filtered.copy()
            df_filtered.loc[:, "A"] = pd.to_datetime(df_filtered["A"], errors="coerce")
            df_filtered = df_filtered[df_filtered["A"] >= pd.to_datetime(start_date)]
            print(f"After Date filter (A >= {start_date}): {len(df_filtered):,}")
        else:
            print("Skipping Date filter: 'A' not found")

        if df_filtered.empty:
            print("No records remain after filtering.")
            return pd.DataFrame()

        # Optional: round forecast quantity if 'B' exists
        if "B" in df_filtered.columns:
            df_filtered = df_filtered.copy()
            df_filtered.loc[:, "B"] = pd.to_numeric(df_filtered["B"], errors="coerce").round(1)
            print("Rounded 'B' (forecast quantity) to 1 decimal place.")

        # Sort by common keys if present (keeps all columns)
        sort_cols = [c for c in ["I", "S", "CSC2", "A"] if c in df_filtered.columns]
        if sort_cols:
            df_final = df_filtered.sort_values(sort_cols)
            print(f"Sorted by: {sort_cols}")
        else:
            df_final = df_filtered

        # Save ALL fields to CSV
        filename = f"Forecast Analysis - Bottom Level {datetime.now().strftime('%Y-%m-%d')}.csv"
        df_final.to_csv(filename, index=False)
        print(f"Saved: {filename}")
        print(f"Final rows: {len(df_final):,}, columns: {len(df_final.columns)}")

        # Preview (truncated)
        with pd.option_context("display.max_columns", 12, "display.max_colwidth", 25):
            print("Preview:")
            print(df_final.head(5).to_string(index=False))

        return df_final

    except requests.exceptions.ConnectionError as e:
        print(f"Connection Error: {e}")
        return None
    except requests.exceptions.Timeout as e:
        print(f"Timeout Error: {e}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Request Error: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None


def main():
    print("StockIQ Bottom Level Forecast - All Fields (No field_mappings)")
    print("Filters: I startswith '10'; ISC4 in ABC list; ISC1 in categories; S in sites; A >= 2024-07-01")
    result = get_bottom_level_forecast_data()
    if result is None:
        print("Process failed.")
    elif result.empty:
        print("Process completed: no rows after filtering.")
    else:
        print("Process completed successfully.")


if __name__ == "__main__":
    main()