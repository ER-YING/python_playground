import pandas as pd
from calendar import monthrange

def process_forecast(forecast_df):
    """Process forecast data to calculate daily forecasts."""
    # Melt to long format and clean data
    daily_forecast_df = forecast_df.melt(
        id_vars=['Item'], 
        var_name='Month Year', 
        value_name='Monthly Forecast'
    ).dropna(subset=['Monthly Forecast'])
    
    # Convert to datetime and calculate days in month
    daily_forecast_df['Month Year'] = pd.to_datetime(daily_forecast_df['Month Year'])
    daily_forecast_df['Days in Month'] = daily_forecast_df['Month Year'].apply(
        lambda x: monthrange(x.year, x.month)[1]
    )
    
    # Calculate daily forecast and format
    daily_forecast_df['Daily Forecast'] = daily_forecast_df['Monthly Forecast'] / daily_forecast_df['Days in Month']
    daily_forecast_df['Month Year'] = daily_forecast_df['Month Year'].dt.strftime('%b %Y')
    
    return daily_forecast_df[['Item', 'Month Year', 'Days in Month', 'Daily Forecast']]

# Read all sheets from the Excel file
file_path = "Inventory - Northland Goreway.xlsx"
sheets = pd.read_excel(file_path, sheet_name=None)

# Process both forecast sheets
northland_daily = process_forecast(sheets['Northland Goreway Forecast'])
progressive_daily = process_forecast(sheets['Progressive UK Forecast'])
rhenus_daily = process_forecast(sheets['Rhenus Netherlands Forecast'])

# Write to Excel with sheet replacement
with pd.ExcelWriter(
    file_path,
    engine='openpyxl',
    mode='a',  # Append mode
    if_sheet_exists='replace'  # Overwrite existing sheets
) as writer:
    # Save Northland forecast
    northland_daily.to_excel(
        writer, 
        sheet_name='Northland Daily Forecast', 
        index=False
    )
    progressive_daily.to_excel(
        writer, 
        sheet_name='Progressive UK Daily Forecast', 
        index=False
    )
    # Save Rhenus Netherlands forecast
    rhenus_daily.to_excel(
        writer, 
        sheet_name='Rhenus Netherlands Daily Forecast', 
        index=False
    )

print("Successfully updated daily forecasts for both warehouses!")