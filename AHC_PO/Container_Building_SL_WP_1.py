import pandas as pd
import math
import os

#=================================================
#       File path definitions (adjust as needed)
#=================================================
input_file_summary = '/Users/yingji/Documents/python/vs_code/python_playground/AHC_PO/Summary_Cleaned.xlsx'
input_file_sku = '/Users/yingji/Documents/python/vs_code/python_playground/AHC_PO/Sku Dictionary.xlsx'
input_file_storage = '/Users/yingji/Documents/python/vs_code/python_playground/AHC_PO/ShippingTiHi.xlsx'
input_file_urgent = '/Users/yingji/Documents/python/vs_code/python_playground/AHC_PO/Urgent_Items.xlsx'
output_file = '/Users/yingji/Documents/python/vs_code/python_playground/AHC_PO/Container_Building_Output_6.xlsx'

#=================================================
#     Read Excel: Summary, SKU, StorageTiHi, Urgent Items
#=================================================

summary_df = pd.read_excel(os.path.expanduser(input_file_summary))
supplier = summary_df.iloc[0]['Supplier']  # from first row
storage_tihi_df = pd.read_excel(os.path.expanduser(input_file_storage))
sku_dict_df = pd.read_excel(os.path.expanduser(input_file_sku))
urgent_df = pd.read_excel(os.path.expanduser(input_file_urgent))

# Build (location, item) -> urgent quantity map
urgent_items = {(row['Location'], row['Item']): row['Urgent Quantity'] for _, row in urgent_df.iterrows()}

# Build (location, item) -> cases_per_pallet map
cases_per_pallet_map = {}
for _, row in storage_tihi_df.iterrows():
    loc = row['Location']
    it = row['Item']
    c_per_pal = row['Cases per pallet']
    cases_per_pallet_map[(loc, it)] = c_per_pal

#=================================================
#          Constraints & constants
#=================================================

# Define item categories for American Hygienics Corporation
SCENTED_ITEMS = {'10CA11WP0001', '10CA11WP0003', '10CA11WP00027', '10CA11WP0033'}
UNSCENTED_ITEMS = {'10CA11WP0002', '10CA11WP0004', '10CA11WP0028', '10CA11WP0034'}

# Define the locations for this special logic
AHS_LOCATIONS = {'Source NJ', 'Source Montebello', 'Golden State FC LLC (AGL)', 'Northland Goreway'}

if loc == 'Yanada':
    CONTAINER_CBM_LIMIT = float('inf')  # No CBM limit
else:
    CONTAINER_CBM_LIMIT = 68

LOCATION_WEIGHT_LIMITS = {
    'Northland Goreway': 25000,
    'Progressive UK': 26000,
    'Rhenus Netherlands': 26000,
    'Source Montebello': 23500,
    'Source NJ': 24000,
    'Golden State FC LLC (AGL)': 19878,
    'Yanada': float('inf')  # No weight limit
}
ITEMS_PER_CONTAINER = 17

def get_weight_limit(location):
    return LOCATION_WEIGHT_LIMITS.get(location, 25000)

#=================================================
#    Clean & check columns
#=================================================
sku_dict_df.columns = sku_dict_df.columns.str.strip().str.lower()
required_cols = ['item', 'units per case/carton', 'unit weight', 'cbm per unit']
for col in required_cols:
    if col not in sku_dict_df.columns:
        raise KeyError(f"Missing required column in SKU dictionary: {col}")

summary_df.columns = summary_df.columns.str.strip()
summary_df = summary_df[(summary_df['Item'].notnull()) & (summary_df['Item'] != 0)]

# Keep only needed columns from SKU
sku_dict_df = sku_dict_df[['item','units per case/carton','unit weight','cbm per unit']]

#=================================================
#   Merge summary with SKU
#=================================================
merged_df = pd.merge(summary_df, sku_dict_df, left_on='Item', right_on='item', how='left')

# Convert numeric columns
merged_df['Quantity'] = pd.to_numeric(merged_df['Suggested Quantity'], errors='coerce').fillna(0)
merged_df['units per case/carton'] = pd.to_numeric(merged_df['units per case/carton'], errors='coerce').fillna(1)
merged_df['unit weight'] = pd.to_numeric(merged_df['unit weight'], errors='coerce').fillna(0)
merged_df['cbm per unit'] = pd.to_numeric(merged_df['cbm per unit'], errors='coerce').fillna(0)

# (Optional) compute or store columns like 'Cases Needed', 'Density', etc.
merged_df['Cases Needed'] = (merged_df['Quantity']/merged_df['units per case/carton']).apply(math.ceil)
merged_df['Total Weight'] = merged_df['Cases Needed'] * merged_df['units per case/carton'] * merged_df['unit weight']
merged_df['Total CBM']    = merged_df['Cases Needed'] * merged_df['units per case/carton'] * merged_df['cbm per unit']

#=================================================
#   Separate urgent and non-urgent items
#=================================================
urgent_df = merged_df[merged_df.apply(lambda row: (row['Location'], row['Item']) in urgent_items, axis=1)]
non_urgent_df = merged_df[~merged_df.apply(lambda row: (row['Location'], row['Item']) in urgent_items, axis=1)]

# Update urgent_df quantities to match urgent quantities specified
for index, row in urgent_df.iterrows():
    key = (row['Location'], row['Item'])
    if key in urgent_items:
        urgent_df.at[index, 'Quantity'] = urgent_items[key]
        remaining_quantity = row['Quantity'] - urgent_items[key]
        if remaining_quantity > 0:
            new_row = row.copy()
            new_row['Quantity'] = remaining_quantity
            non_urgent_df = pd.concat([non_urgent_df, pd.DataFrame([new_row])], ignore_index=True)

#=================================================
#   Container & Pallet-based allocation
#=================================================
containers = []
container_number = 1


def open_new_container():
    """
    Returns a new container dict with no items, ready for usage.
    We'll fill in the rest once we know location, etc.
    """
    return {
        'Container#': None,   # assign later
        'Supplier': supplier,
        'Location': None,
        'Items': [],
        'Total Weight': 0.0,
        'Total CBM': 0.0,
        'Weight Cap': 0.0,
        'CBM Cap': 0.0
    }


def can_fit_pallets(container, pallets_to_add, item_dict, c_per_pal):
    """
    Check if 'pallets_to_add' (may be float for partial leftover)
    can fit in the container by weight & CBM constraints.
    """
    upc = item_dict['units per case/carton']
    wpu = item_dict['unit weight']
    cpu = item_dict['cbm per unit']

    # total cartons = pallets_to_add * c_per_pal
    # total units   = that * upc
    total_cartons = pallets_to_add * c_per_pal
    weight_needed = total_cartons * upc * wpu
    cbm_needed = total_cartons * upc * cpu

    if container['Total Weight'] + weight_needed <= container['Weight Cap'] and \
            container['Total CBM'] + cbm_needed <= container['CBM Cap'] and \
            len(container['Items']) < ITEMS_PER_CONTAINER:
        return True
    return False


def add_pallets_to_container(container, pallets_to_add, item_dict, c_per_pal):
    """
    Actually update container with that many full or partial pallets for the item.
    """
    upc = item_dict['units per case/carton']
    wpu = item_dict['unit weight']
    cpu = item_dict['cbm per unit']

    total_cartons = pallets_to_add * c_per_pal
    weight_to_add = total_cartons * upc * wpu
    cbm_to_add = total_cartons * upc * cpu

    container['Total Weight'] += weight_to_add
    container['Total CBM'] += cbm_to_add

    # Update or create an entry in container['Items']
    found = None
    for i in container['Items']:
        if i['Item'] == item_dict['Item']:
            found = i
            break
    if found:
        found['Quantity'] += total_cartons * upc  # total units
        found['Weight'] += weight_to_add
        found['CBM'] += cbm_to_add
        found['Cartons'] += total_cartons
    else:
        container['Items'].append({
            'Item': item_dict['Item'],
            'Quantity': total_cartons * upc,  # units
            'Weight': weight_to_add,
            'CBM': cbm_to_add,
            'Cartons': total_cartons
        })


#=================================================
#    Combined weight+volume heuristic (alpha=0.5)
#=================================================
alpha = 0.5

for location, loc_items_df in pd.concat([urgent_df, non_urgent_df]).groupby('Location'):
    location_weight_limit = get_weight_limit(location)

    # For each item, compute a combined_score
    # = alpha*(unit_weight/location_weight_limit) + (1-alpha)*(cbm_per_unit/CONTAINER_CBM_LIMIT)
    loc_items_df['combined_score'] = (
        alpha * (loc_items_df['unit weight'] / location_weight_limit)
        + (1 - alpha) * (loc_items_df['cbm per unit'] / CONTAINER_CBM_LIMIT)
    )

    # =================================================
    #   >>> NEW LOGIC: Conditional sorting by category <<<
    # =================================================
    if supplier == 'American Hygienics Corporation' and location in AHS_LOCATIONS:
        # Define a function to assign a category to an item
        def get_category(item):
            if item in SCENTED_ITEMS:
                return 'A_Scented'  # 'A_' prefix ensures it's sorted first
            elif item in UNSCENTED_ITEMS:
                return 'B_Unscented'
            else:
                return 'C_Other'

        # Create a new 'Category' column
        loc_items_df['Category'] = loc_items_df['Item'].apply(get_category)

        # Sort by Category first, then by the original score
        loc_items_df = loc_items_df.sort_values(by=['Category', 'combined_score'], ascending=[True, False])
    else:
        # Original logic: Sort by score only for all other suppliers/locations
        loc_items_df = loc_items_df.sort_values(by='combined_score', ascending=False)

    # Separate urgent and non-urgent containers for priority processing
    urgent_items_df = loc_items_df[loc_items_df.apply(lambda row: (row['Location'], row['Item']) in urgent_items, axis=1)]
    non_urgent_items_df = loc_items_df[~loc_items_df.apply(lambda row: (row['Location'], row['Item']) in urgent_items, axis=1)]

    location_containers = []
    current_container = None

    # Process urgent items first
    for df in [urgent_items_df, non_urgent_items_df]:
        for _, item_row in df.iterrows():
            # 1) compute total CARTONS for this item
            upc = item_row['units per case/carton']
            if upc < 1:
                upc = 1
            total_cartons = item_row['Quantity'] / upc  # how many "cartons"

            # 2) from storage_tihi: c_per_pal
            c_per_pal = cases_per_pallet_map.get((location, item_row['Item']), 1)
            if c_per_pal < 1:
                c_per_pal = 1

            # total pallets (float)
            total_pallets_float = total_cartons / c_per_pal
            leftover_pallets = total_pallets_float

            #-----------------------------------------
            #  Allocate FULL pallets first
            #-----------------------------------------
            while leftover_pallets >= 1:
                if current_container is None:
                    current_container = open_new_container()
                    current_container['Container#'] = container_number
                    current_container['Location'] = location
                    current_container['Supplier'] = supplier
                    current_container['Weight Cap'] = location_weight_limit
                    current_container['CBM Cap'] = CONTAINER_CBM_LIMIT
                    container_number += 1

                full_pallets_avail = math.floor(leftover_pallets)

                # Check if we can fit at least 1 pallet
                if not can_fit_pallets(current_container, 1, item_row, c_per_pal):
                    # close container, open a new one
                    location_containers.append(current_container)
                    current_container = open_new_container()
                    current_container['Container#'] = container_number
                    current_container['Location'] = location
                    current_container['Supplier'] = supplier
                    current_container['Weight Cap'] = location_weight_limit
                    current_container['CBM Cap'] = CONTAINER_CBM_LIMIT
                    container_number += 1

                    # if can't fit 1 pallet in an empty container => skip or break
                    if not can_fit_pallets(current_container, 1, item_row, c_per_pal):
                        print(f"WARNING: Single pallet of {item_row['Item']} doesn't fit an empty container at {location}.")
                        leftover_pallets = 0
                        break

                # how many pallets can we place at once?
                # naive approach: try full_pallets_avail in descending order
                placed = 0
                for possible_count in range(full_pallets_avail, 0, -1):
                    if can_fit_pallets(current_container, possible_count, item_row, c_per_pal):
                        add_pallets_to_container(current_container, possible_count, item_row, c_per_pal)
                        placed = possible_count
                        break
                leftover_pallets -= placed

                if leftover_pallets < 1:
                    break  # done full pallets => might have partial leftover
                # if we still have leftover_pallets >=1 => continue in this container if possible

            #-----------------------------------------
            #  If leftover_pallets < 1 => partial leftover
            #-----------------------------------------
            if leftover_pallets > 0:
                # we have a partial pallet leftover
                if current_container is None:
                    current_container = open_new_container()
                    current_container['Container#'] = container_number
                    current_container['Location'] = location
                    current_container['Supplier'] = supplier
                    current_container['Weight Cap'] = location_weight_limit
                    current_container['CBM Cap'] = CONTAINER_CBM_LIMIT
                    container_number += 1

                if not can_fit_pallets(current_container, leftover_pallets, item_row, c_per_pal):
                    # finalize and open new container
                    location_containers.append(current_container)
                    current_container = open_new_container()
                    current_container['Container#'] = container_number
                    current_container['Location'] = location
                    current_container['Supplier'] = supplier
                    current_container['Weight Cap'] = location_weight_limit
                    current_container['CBM Cap'] = CONTAINER_CBM_LIMIT
                    container_number += 1
                    if not can_fit_pallets(current_container, leftover_pallets, item_row, c_per_pal):
                        print(f"WARNING: partial leftover pallet {round(leftover_pallets, 3)} of {item_row['Item']} doesn't fit even in empty container at {location}. Skipping leftover.")
                        leftover_pallets = 0

                # if still leftover
                if leftover_pallets > 0:
                    add_pallets_to_container(current_container, leftover_pallets, item_row, c_per_pal)
                leftover_pallets = 0

        # finalize the last container if not empty
        if current_container is not None:
            location_containers.append(current_container)
            current_container = None

    # add to global containers
    containers.extend(location_containers)


#=================================================
#   Prepare final output
#=================================================
output_data = []

for container in containers:
    cid = container['Container#']
    loc = container['Location']
    supp = container['Supplier']
    tot_wt = container['Total Weight']
    tot_cbm = container['Total CBM']
    itemlist = container['Items']

    for idx, it in enumerate(itemlist):
        c_per_pal = cases_per_pallet_map.get((loc, it['Item']), 0)
        if c_per_pal == 0:
            num_pal = 0
        else:
            # Number of pallet = Cartons / c_per_pal
            num_pal = round(it['Cartons'] / c_per_pal, 2)

        output_data.append({
            'Container#': cid,
            'Supplier': supp,
            'Location': loc,
            'Item': it['Item'],
            'Quantity': it['Quantity'],
            'Weight': round(it['Weight'], 2),
            'CBM': round(it['CBM'], 2),
            'Cartons': it['Cartons'],
            'Cases per pallet': c_per_pal,
            'Number of pallet': num_pal,
            'Total Weight': '' if idx > 0 else round(tot_wt, 2),
            'Total CBM': '' if idx > 0 else round(tot_cbm, 2)
        })

final_df = pd.DataFrame(output_data)
final_df.to_excel(os.path.expanduser(output_file), index=False)
print(f"Done! Created {len(containers)} containers with combined-heuristic item sort + pallet-based allocation.\nOutput: {output_file}")
