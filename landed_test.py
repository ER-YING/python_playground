import pandas as pd
from datetime import date



current_date = date.today()

formatted_date = current_date.strftime("%Y-%m-%d")  

input_number = 1501
customs_brokerage_fee_local_currency = 95
customer_fob_cost_usd = 7203.47
additional_duty = 0
customize_input_occean_rate = 6254
customize_input_drayage_rate = 999.47
exchange_rate_usd_to_local_currency = 0.9287

workbook = pd.ExcelFile('../Playwright_expercice/test.xlsx')
quantities = workbook.parse('Sheet1')
quantities_corrected = quantities.copy()  # This is a placeholder; adjust cleaning steps as needed
warehouse = ""
for _, item in quantities_corrected.iterrows():
    if input_number == item['Internal Id']:
        warehouse = item['Destination WH']
        break
catalogue_mapping = pd.DataFrame()
sheet_data = workbook.parse(warehouse)[['ITEM#', 'Catalogue']]
catalogue_mapping = pd.concat([catalogue_mapping, sheet_data])
catalogue_mapping = catalogue_mapping.drop_duplicates(subset=['ITEM#'])
quantities_corrected = pd.merge(quantities, catalogue_mapping, how='left', left_on='Item', right_on='ITEM#')
unit_prices_mapping_corrected = {}

sheet_data = workbook.parse(warehouse)
for _, row in sheet_data.iterrows():
    item_id = row['ITEM#']
    unit_price = row['Unit Price']  # Assuming 'Unit Price' column exists and is named consistently
    unit_prices_mapping_corrected[item_id] = unit_price

# Accumulate total prices by catalogue
catalogue_totals = {}
duty_data = workbook.parse('Duty', index_col=0)  # Set the first column as the row index
# Transpose the duty data if necessary, and convert it into a dictionary
# This step depends heavily on how your 'Duty' sheet is structured

duty_rates_uk = {}  # This will be a dictionary mapping catalogues to their duty rates for the UK warehouse
duty_rates_montebello = {}
duty_rates_nj = {}
duty_rates_northland = {}
duty_rates_Rhenus = {}

for category in duty_data.columns:
    duty_rates_uk[category] = duty_data.at['Progressive UK', category]
    duty_rates_montebello[category] = duty_data.at['Source Montebello', category]
    duty_rates_nj[category] = duty_data.at['Source NJ', category]
    duty_rates_northland[category] = duty_data.at['Northland Goreway', category]
    duty_rates_Rhenus[category] = duty_data.at['Rhenus Netherlands', category]

duty_rates = {
    'Progressive UK': duty_rates_uk,
    'Source Montebello': duty_rates_montebello,
    'Source NJ': duty_rates_nj,
    'Northland Goreway': duty_rates_northland,
    'Rhenus Netherlands': duty_rates_Rhenus,
}

# Assuming quantities_corrected is a DataFrame that includes the necessary columns like 'Internal Id', 'Volume', 'Weight', etc.
# Assuming unit_prices_mapping_corrected is a dictionary with item catalogues as keys and unit prices as values
# Assuming duty_rates_uk is a dictionary with catalogue categories as keys and duty rates as values
# Assuming sku_to_catalogue_mapping is a dictionary mapping SKUs to catalogue categories

# Correct Python syntax and logic for the given pseudo-code
extra_calculation = False
biggest_duty = 0
needCalculateCurrency = False
isUSWarehouse = False

if warehouse in ["Progressive UK", "Rhenus Netherlands"]:
    extra_calculation = True

if warehouse in["Progressive UK", "Rhenus Netherlands","Northland Goreway"]:
    needCalculateCurrency = True

if warehouse in["Source Montebello","Source NJ"]:
    isUSWarehouse = True

# Sum up total volume and weight
filtered_quantities = quantities_corrected[quantities_corrected['Internal Id'] == input_number]
total_volume = filtered_quantities['Volume'].sum()
total_weight = filtered_quantities['Weight'].sum()

# Determine calculation basis
calculation_basis = 'volume' if total_volume > 65 else 'weight'
total_basis = total_volume if calculation_basis == 'volume' else total_weight
recalculated_costs = []
catalogues = set()


duty_total = 0
total_price = 0
total_price_usd = 0
totalCost = 0
# Calculate and print proportion for each item
for _, item in filtered_quantities.iterrows():    
    proportion = (item['Volume'] if calculation_basis == 'volume' else item['Weight']) / total_basis
    fob_weight_proportion = item['Weight']/total_weight
    unit_price = unit_prices_mapping_corrected.get(item['Item'], 0)  # Assume a corrected mapping is used
    total_price += item['Qty'] * unit_price
    total_price_usd += item['Qty'] * unit_price
    print('unit total usd - ' + str(item['Qty'] * unit_price))
    print(f"Item: {item['Item']}, Proportion: {proportion:.2%}, FOB Weight Proportion: {fob_weight_proportion:.2%}")
    # print(item['Weight'])
    # print(total_weight)

# Collect catalogues and find the biggest duty rate if extra calculations are needed
    if extra_calculation:
        for _, items in filtered_quantities.iterrows():
            catalogue = items['Catalogue']
            duty_rate = duty_rates[warehouse].get(catalogue, 0)
            catalogues.add(catalogue)

    totalCost +=  item['Qty'] * unit_price
    #print(filtered_quantities.iterrows())
# for _, item in filtered_quantities.iterrows():
    if warehouse == "Progressive UK":
        # unit_price = unit_prices_mapping_corrected.get(item['Item'], 0)  # Assume a corrected mapping is used
        fob_proportion = unit_price*item['Qty']/total_price
    if warehouse == "Rhenus Netherlands":
        fob_weight_proportion = item['Weight']/total_weight
    # proportion = (item['Volume'] if calculation_basis == 'volume' else item['Weight']) / total_basis
    # unit_price = unit_prices_mapping_corrected.get(item['Item'], 0)
    # print(fob_weight_proportion)  # Assume a corrected mapping is used
    # print(item['Qty'])
    
    
    duty_rate = duty_rates[warehouse].get(item['Catalogue'], 0)
    item_specific_duty = item['Qty'] * unit_price * duty_rate
    brokerage_fee_allocation = customs_brokerage_fee_local_currency * proportion
    additional_duty_allocation = additional_duty * proportion
    us_processing_fee_allocation = 0

    
    # total_us_processing_fee = 0
    # us_processing_fee = 0
    # if isUSWarehouse:
    #  calculated_fee = item['Qty'] * unit_price * 0.4714 / 100  # Calculate here
    #  us_processing_fee_allocation = calculated_fee if calculated_fee > 31.67 else 31.67 

    if isUSWarehouse:
       us_processing_fee_allocation = item['Qty'] * unit_price * 0.4714 / 100
    #    total_us_processing_fee += us_processing_fee_allocation
    #    us_processing_fee = us_processing_fee_allocation if total_us_processing_fee >31.67 else 31.67

    # if us_processing_fee_allocation <= 31.67:
    #    us_processing_fee_allocation = 31.67

    # Determine whether to apply extra calculations based on the warehouse
    if warehouse == "Progressive UK":
        fob_cost_allocation = customer_fob_cost_usd * duty_rate * fob_proportion
        final_duty_cost_usd = fob_cost_allocation + item_specific_duty
    elif warehouse == "Rhenus Netherlands":
        fob_cost_allocation = customer_fob_cost_usd * duty_rate * fob_weight_proportion
        final_duty_cost_usd = fob_cost_allocation + item_specific_duty 

    else:
       final_duty_cost_usd = item_specific_duty + us_processing_fee_allocation
     
    # Convert final duty cost to local currency
    if needCalculateCurrency:
        final_duty_cost_usd_convert_to_local = final_duty_cost_usd * exchange_rate_usd_to_local_currency
        final_duty_cost_usd_to_local = final_duty_cost_usd_convert_to_local + brokerage_fee_allocation + additional_duty_allocation
    else:
        final_duty_cost_usd_to_local = final_duty_cost_usd + brokerage_fee_allocation +additional_duty_allocation
    #print(fob_cost_allocation)
    #print(final_duty_cost_usd_to_local)
    final_ocean_rate = proportion * customize_input_occean_rate
    final_drayage_rate = proportion * customize_input_drayage_rate
    print('final ocean rate - ' + str(final_ocean_rate))
    print('final drayage rate - ' + str(final_drayage_rate))
    #print(final_duty_cost_usd_to_local)
    duty_total += (final_duty_cost_usd_to_local - brokerage_fee_allocation)
    recalculated_costs.append({
        'PO#': item['PO#'],
        'Item': item['Item'],
        'Catalogue': item['Catalogue'],
        'Quantity': item['Qty'],
        'Unit Price': unit_price,
        'Total Price USD': total_price_usd,
        'Ocean Freight Cost USD': final_ocean_rate,
        'Drayage Cost USD': final_drayage_rate,
        'Final Duty Cost Local Currency': final_duty_cost_usd_to_local,
    })
print('totalCost - ' + str(totalCost))
print('total price usd - ' + str(total_price_usd))
print('duty total - ' + str(duty_total))
# Prepare the DataFrame from your data
        
restructured_data = []
input_number_str = str(input_number)
currency = "US Dollar"
if warehouse == "Northland Goreway":
    currency = "CAN"
elif warehouse == "Progressive UK":
    currency = "British pound"
elif warehouse == "Rhenus Netherlands":
    currency = "Euro"


total = 0
for cost in recalculated_costs:
    total += cost['Final Duty Cost Local Currency']
    restructured_data.extend([
        {'Internal ID': input_number_str, 'Shipment Number': "INBSHIP" + input_number_str, 'Items - Item': cost['Item'], 'Items - PO': cost['PO#'], 'PO # Line': "PO#"+ cost['PO#'], "PO#ItemLine": "PO#"+ cost['PO#'] + " " + cost['Item'], "Landed Cost - Allocation Method" : "Quantity", 'Landed Cost - Amount': cost['Ocean Freight Cost USD'], "Landed Cost - Cost Category" : "Inbound Freight", "Landed Cost - Currency": "US Dollar", "Landed Cost - Effective Date" : formatted_date},
        {'Internal ID': input_number_str, 'Shipment Number': "INBSHIP" + input_number_str, 'Items - Item': cost['Item'], 'Items - PO': cost['PO#'], 'PO # Line': "PO#"+ cost['PO#'], "PO#ItemLine": "PO#"+ cost['PO#'] + " " + cost['Item'], "Landed Cost - Allocation Method" : "Quantity", 'Landed Cost - Amount': cost['Drayage Cost USD'], "Landed Cost - Cost Category" : "Inbound Freight", "Landed Cost - Currency": "US Dollar", "Landed Cost - Effective Date" : formatted_date},
        {'Internal ID': input_number_str, 'Shipment Number': "INBSHIP" + input_number_str, 'Items - Item': cost['Item'], 'Items - PO': cost['PO#'], 'PO # Line': "PO#"+ cost['PO#'], "PO#ItemLine": "PO#"+ cost['PO#'] + " " + cost['Item'], "Landed Cost - Allocation Method" : "Quantity", 'Landed Cost - Amount': cost['Final Duty Cost Local Currency'], "Landed Cost - Cost Category" : "Inbound Duties", "Landed Cost - Currency": currency, "Landed Cost - Effective Date" : formatted_date}
    ])
restructured_costs_df = pd.DataFrame(restructured_data)
#     Create a filename using the input number
file_path = f"{input_number_str} Landed Cost Extract.csv"
restructured_costs_df.to_csv('../Playwright_expercice/'+file_path, index=False)
file_path = f"{input_number_str} Landed Cost Extract.xlsx"
restructured_costs_df.to_excel('../Playwright_expercice/'+file_path, sheet_name='Sheet 1', index=False)

# # Using 'openpyxl' to load the workbook
# book = load_workbook('test.xlsx')
# writer = pd.ExcelWriter('test.xlsx', engine='openpyxl')
# print(writer)
# writer.book = book
# restructured_costs_df.to_excel(writer, sheet_name='Final Result', index=False)
# writer.save()
# writer.close()



print("New sheet 'Final Result' has been added to 'test.xlsx'.")

input_data = {
    'Input Number': [input_number],
    'Customs Brokerage Fee (Local Currency)': [customs_brokerage_fee_local_currency],
    'Customer FOB Cost (USD)': [customer_fob_cost_usd],
    'Additional Duty': [additional_duty],
    'Customize Input Ocean Rate': [customize_input_occean_rate],
    'Customize Input Drayage Rate': [customize_input_drayage_rate],
    'Exchange Rate (USD to Local Currency)': [exchange_rate_usd_to_local_currency],
    'Total Duty without Customs Brokerage Fee': [duty_total]
}
input_df = pd.DataFrame(input_data)

file_path = f"{input_number_str} Input Ref.csv"
input_df.to_csv('../Playwright_expercice/'+file_path, index=False)

print(f"Input fields saved to {'../Playwright_expercice/'+file_path}") 
