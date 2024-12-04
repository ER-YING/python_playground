import pandas as pd
from datetime import datetime

# Load the CSV file
data = pd.read_csv('../Playwright_expercice/Shipments.csv')

# Function to extract the numeric part from the 'Shipment Name' column
def extract_number(name):
    return name.replace('INBSHIP', '')

#Function to convert and format dates in the specified columns
def convert_and_format_dates(dataframe, column_name):
    try:
        # Convert to datetime, ignoring errors and assuming UTC for timezone-aware conversion
        formatted_dates = pd.to_datetime(dataframe[column_name], errors='coerce', utc=True)
        # Convert datetime objects to date strings in 'yyyy-mm-dd' format
        dataframe[column_name] = formatted_dates.dt.strftime('%Y-%m-%d')
    except Exception as e:
        print(f"Error processing column {column_name}: {e}")

# Update 'Shipment Name' column
data['Shipment Name'] = data['Shipment Name'].apply(extract_number)

# List of date columns to format
date_columns = ['Actual Pickup Date', 'Estimated Departure Date', 'Actual Departure Date', 'Estimated Discharge Date', 'Actual Discharge Date', 'Actual Gate Out Date', 'Actual Empty Return Date','Revised Estimated Arrival Date']

# Apply date formatting to each column
for col in date_columns:
    convert_and_format_dates(data, col)

# Function to combine 'Current Vessel Name' and 'Current Voyage No'
def combine_vessel_and_voyage(dataframe):
    dataframe['Vessel Number'] = dataframe['Current Vessel Name'] + ' ' + dataframe['Current Voyage No']

# Combine 'Current Vessel Name' and 'Current Voyage No' into 'Vessel Number'
combine_vessel_and_voyage(data)

# Drop 'Current Vessel Name' and 'Current Voyage No' columns
data.drop(columns=['Current Vessel Name', 'Current Voyage No'], inplace=True)

# Create a new filename using today's date and "OEC Tracking"
today_date = datetime.now().strftime('%Y-%m-%d')
filename = f"{today_date}_OEC_Tracking.csv"

# Save the updated DataFrame to a new CSV file
data.to_csv('../Playwright_expercice/'+filename, index=False)

print(f"Data saved to {filename}")
