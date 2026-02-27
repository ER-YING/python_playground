import pandas as pd

# Load the data from the Excel file
file_path = '../Playwright_expercice/Unit Turn Clean Data.xlsx'  # Change this to your actual file path
data = pd.read_excel(file_path)

# Convert 'Period Date' to datetime and sort the data by this date
data['Period Date'] = pd.to_datetime(data['Period Date'])
data_sorted = data.sort_values('Period Date', ascending=False)  # Sort descending to make recent dates first

# Calculate the relative position of each date to the most recent date in the dataset
most_recent_date = data_sorted['Period Date'].max()
data_sorted['Months Since Most Recent'] = (most_recent_date.year - data_sorted['Period Date'].dt.year) * 12 + \
                                         (most_recent_date.month - data_sorted['Period Date'].dt.month)

# Filter out the data to include only the most recent 12 months for the 'Custom Quarter' calculations
data_recent_12_months = data_sorted[data_sorted['Months Since Most Recent'] < 12].copy()

# Correct usage with .loc to avoid SettingWithCopyWarning
data_recent_12_months.loc[:, 'Custom Quarter'] = (data_recent_12_months['Months Since Most Recent'] // 3) % 4 + 1

# Use .loc for transforming and retaining the first occurrence of each necessary column
data_recent_12_months.loc[:, 'Product Category'] = data_recent_12_months.groupby(['Site', 'Item'])['Product Category'].transform('first')
data_recent_12_months.loc[:, 'NetSuite ABC'] = data_recent_12_months.groupby(['Site', 'Item'])['NetSuite ABC'].transform('first')

# Group by 'Site', 'Item', 'Custom Quarter', 'Product Category', 'NetSuite ABC' and calculate the average 'Unit Turns'
quarterly_turns_recent = data_recent_12_months.groupby(
    ['Site', 'Item', 'Custom Quarter', 'Product Category', 'NetSuite ABC']
)['Unit Turns'].mean().reset_index()

# Save the processed data to an Excel file
output_file_path = '../Playwright_expercice/Quarter Data.xlsx'  # Change this to your desired output path
quarterly_turns_recent.to_excel(output_file_path, index=False)

print("Data has been processed and saved successfully.")
