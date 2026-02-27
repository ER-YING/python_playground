import pandas as pd
import requests
import re
from datetime import datetime, timedelta
import json

def extract_number(cust_order_id):
    """Extract all digits from cust_order_id"""
    if pd.isna(cust_order_id) or cust_order_id is None:
        return ''
    return ''.join(re.findall(r'\d+', str(cust_order_id)))

def format_datetime(dt_str):
    """Convert datetime string to yyyy-mm-dd format"""
    try:
        return dt_str.split()[0] if dt_str else ''
    except (AttributeError, TypeError):
        return ''

def calculate_expected_delivery_date(pod_code, pod_ata, pod_eta):
    """Calculate expected delivery date based on pod_code and pod_ata (fallback to pod_eta)"""
    # Determine which date to use
    base_date = pod_ata if pod_ata and pod_ata != '' else pod_eta
    
    if not base_date or base_date == '':
        return ''
    
    try:
        # Parse the base date
        date_obj = datetime.strptime(base_date, '%Y-%m-%d')
        
        # Define days to add based on pod_code
        days_to_add = 0
        if pod_code in ['USLAX', 'USLGB', 'USOAK', 'USNYC']:
            days_to_add = 5
        elif pod_code in ['CAPRR', 'CAVAN']:
            days_to_add = 20
        elif pod_code == 'NLRTM':
            days_to_add = 9
        elif pod_code == 'GBLGP':
            days_to_add = 7
        else:
            # For unknown pod_codes, print message and return empty string
            print(f"pod_code is not recognized: {pod_code}")
            return ''
        
        # Calculate expected delivery date
        expected_date = date_obj + timedelta(days=days_to_add)
        return expected_date.strftime('%Y-%m-%d')
        
    except (ValueError, TypeError):
        return ''

def create_pol_atd(pol_atd, loading_on_vessel):
    """Create created_pol_atd based on pol_atd and LOADING_ON_VESSEL"""
    if pol_atd and pol_atd != '':
        return pol_atd
    elif loading_on_vessel and loading_on_vessel != '':
        return loading_on_vessel
    else:
        return ''

def get_tracking_data():
    # Read BL numbers from Excel
    try:
        bl_data = pd.read_excel('Tradlinx Retrieve Shipping Tracking Data.xlsx', sheet_name='Sheet1')
        bl_nos = bl_data['bl_no'].tolist()
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return
    
    # API configuration
    url = 'https://api.tradlinx.com/partners/track/v2/cargo-tracks/query?'
    headers = {
        'tx-clientid': 'EarthRated',
        'tx-apikey': 'MjVmMTQzN2QtMzRlMS0zMjA5LWJlZjYtZTRiNjEwOWQ5Nzkw',
        'Content-Type': 'application/json'
    }
    
    output = []
    
    # Process BL numbers in batches of 50
    for i in range(0, len(bl_nos), 50):
        batch = bl_nos[i:i+50]
        try:
            print(f"Processing batch: {batch}")
            response = requests.get(
                url,
                headers=headers,
                params={'version': 'EA', 'bl_no': ','.join(batch)}
            )
            response.raise_for_status()
            data = response.json()
            
            for cargo in data.get('content', []):
                # Process base data with datetime formatting
                base_data = {
                    'bl_no': cargo.get('bl_no'),
                    'cust_order_id': extract_number(cargo.get('cust_order_id')),
                    'cur_vessel_nm': cargo.get('cur_vessel_nm', 'N/A'),
                    'pol_etd': format_datetime(cargo.get('pol', {}).get('etd')),
                    'pol_atd': format_datetime(cargo.get('pol', {}).get('atd')),
                    'pod_code': cargo.get('pod', {}).get('code', ''),
                    'pod_eta': format_datetime(cargo.get('pod', {}).get('eta')),
                    'pod_ata': format_datetime(cargo.get('pod', {}).get('ata')),
                    'with_extend_tracking': cargo.get('with_extend_tracking', False)
                }
                print(f"Base data: {base_data}")
                
                # Process containers
                containers = cargo.get('containers', [])
                print(f"Number of containers found: {len(containers)}")
                
                if containers:
                    for idx, container in enumerate(containers):
                        print(f"Container {idx + 1}: {container.get('cntr_no', 'N/A')}")
                        
                        # FIXED: Use correct field name 'tracking_point' instead of 'trackingPoint'
                        tracking_points = container.get('tracking_point', [])
                        print(f"  Number of tracking points: {len(tracking_points)}")
                        
                        if tracking_points:
                            print("  Tracking points found:")
                            for tp in tracking_points:
                                print(f"    - Type: {tp.get('type')}, DateTime: {tp.get('event_datetime')}")
                        else:
                            print("  No tracking points found")
                        
                        container_data = {
                            **base_data,
                            'cntr_no': container.get('cntr_no', ''),
                            'CONTAINER_PICK_UP': '',
                            'GATE_IN': '',
                            'GATE_OUT': '',
                            'LOADING_ON_VESSEL': '',
                            'VESSEL_DEPARTURE_FROM_PORT': '',
                            'VESSEL_ARRIVAL_AT_PORT': '',
                            'DISCHARGING_FROM_VESSEL': '',
                            'RAIL_LOADING': '',
                            'RAIL_ARRIVAL': '',
                            'PICKING_UP_BY_CONSIGNEE': '',
                            'EMPTY_CONTAINER_RETURN': ''
                        }
                        
                        # FIXED: Process container events using correct field name
                        events_found = 0
                        for event in container.get('tracking_point', []):
                            event_type = event.get('type')
                            event_datetime = format_datetime(event.get('event_datetime'))
                            
                            print(f"    Processing event: {event_type} = {event_datetime}")
                            
                            # Map all the extended tracking events
                            if event_type == 'CONTAINER_PICK_UP':
                                container_data['CONTAINER_PICK_UP'] = event_datetime
                                events_found += 1
                            elif event_type == 'GATE_IN':
                                container_data['GATE_IN'] = event_datetime
                                events_found += 1
                            elif event_type == 'GATE_OUT':
                                container_data['GATE_OUT'] = event_datetime
                                events_found += 1
                            elif event_type == 'LOADING_ON_VESSEL':
                                container_data['LOADING_ON_VESSEL'] = event_datetime
                                events_found += 1
                            elif event_type == 'VESSEL_DEPARTURE_FROM_PORT':
                                container_data['VESSEL_DEPARTURE_FROM_PORT'] = event_datetime
                                events_found += 1
                            elif event_type == 'VESSEL_ARRIVAL_AT_PORT':
                                container_data['VESSEL_ARRIVAL_AT_PORT'] = event_datetime
                                events_found += 1
                            elif event_type == 'DISCHARGING_FROM_VESSEL':
                                container_data['DISCHARGING_FROM_VESSEL'] = event_datetime
                                events_found += 1
                            elif event_type == 'RAIL_LOADING':
                                container_data['RAIL_LOADING'] = event_datetime
                                events_found += 1
                            elif event_type == 'RAIL_ARRIVAL':
                                container_data['RAIL_ARRIVAL'] = event_datetime
                                events_found += 1
                            elif event_type == 'PICKING_UP_BY_CONSIGNEE':
                                container_data['PICKING_UP_BY_CONSIGNEE'] = event_datetime
                                events_found += 1
                            elif event_type == 'EMPTY_CONTAINER_RETURN':
                                container_data['EMPTY_CONTAINER_RETURN'] = event_datetime
                                events_found += 1
                        
                        # Calculate expected delivery date using pod_ata (fallback to pod_eta)
                        container_data['EXPECTED_DELIVERY_DATE'] = calculate_expected_delivery_date(
                            container_data['pod_code'], 
                            container_data['pod_ata'],
                            container_data['pod_eta']
                        )
                        
                        # Create the new created_pol_atd column
                        container_data['created_pol_atd'] = create_pol_atd(
                            container_data['pol_atd'],
                            container_data['LOADING_ON_VESSEL']
                        )
                        
                        print(f"    Total events captured: {events_found}")
                        output.append(container_data)
                else:
                    # If no containers, add base data only
                    print("  No containers found, adding base data only")
                    base_container_data = {
                        **base_data,
                        'cntr_no': '',
                        'CONTAINER_PICK_UP': '',
                        'GATE_IN': '',
                        'GATE_OUT': '',
                        'LOADING_ON_VESSEL': '',
                        'VESSEL_DEPARTURE_FROM_PORT': '',
                        'VESSEL_ARRIVAL_AT_PORT': '',
                        'DISCHARGING_FROM_VESSEL': '',
                        'RAIL_LOADING': '',
                        'RAIL_ARRIVAL': '',
                        'PICKING_UP_BY_CONSIGNEE': '',
                        'EMPTY_CONTAINER_RETURN': ''
                    }
                    
                    # Calculate expected delivery date for base data using pod_ata (fallback to pod_eta)
                    base_container_data['EXPECTED_DELIVERY_DATE'] = calculate_expected_delivery_date(
                        base_container_data['pod_code'], 
                        base_container_data['pod_ata'],
                        base_container_data['pod_eta']
                    )
                    
                    # Create the new created_pol_atd column for base data
                    base_container_data['created_pol_atd'] = create_pol_atd(
                        base_container_data['pol_atd'],
                        base_container_data['LOADING_ON_VESSEL']
                    )
                    
                    output.append(base_container_data)
                print("---")
                    
        except requests.exceptions.RequestException as e:
            print(f"Error retrieving data for batch {i//50+1}: {e}")
    
    # Create DataFrame and save to CSV
    if output:
        df = pd.DataFrame(output)
        df = df[[
            'bl_no', 'cust_order_id', 'cur_vessel_nm',
            'pol_etd', 'pol_atd', 'pod_code', 'pod_eta', 'pod_ata',
            'cntr_no', 'with_extend_tracking',
            'CONTAINER_PICK_UP', 'GATE_IN', 'GATE_OUT', 
            'LOADING_ON_VESSEL', 'VESSEL_DEPARTURE_FROM_PORT',
            'VESSEL_ARRIVAL_AT_PORT', 'DISCHARGING_FROM_VESSEL',
            'RAIL_LOADING', 'RAIL_ARRIVAL', 
            'PICKING_UP_BY_CONSIGNEE', 'EMPTY_CONTAINER_RETURN',
            'EXPECTED_DELIVERY_DATE', 'created_pol_atd'  # Added the new column
        ]]
        
        # Generate filename with current date
        today = datetime.today().strftime('%Y%m%d')
        filename = f'Tradlinx_Retrieve_Shipping_Tracking_Data_{today}.csv'
        df.to_csv(filename, index=False)
        print(f"Data saved to {filename}")
        print(f"Total records: {len(df)}")
        
        # Check if extended tracking is enabled
        extended_tracking_count = df['with_extend_tracking'].sum()
        print(f"Records with extended tracking: {extended_tracking_count}")
        
        # Check if any events were captured
        event_columns = ['CONTAINER_PICK_UP', 'GATE_IN', 'GATE_OUT', 'LOADING_ON_VESSEL', 
                        'VESSEL_DEPARTURE_FROM_PORT', 'VESSEL_ARRIVAL_AT_PORT', 'DISCHARGING_FROM_VESSEL',
                        'RAIL_LOADING', 'RAIL_ARRIVAL', 'PICKING_UP_BY_CONSIGNEE', 'EMPTY_CONTAINER_RETURN']
        total_events = 0
        for col in event_columns:
            events_in_col = (df[col] != '').sum()
            if events_in_col > 0:
                print(f"{col}: {events_in_col} events found")
            total_events += events_in_col
        
        if total_events == 0:
            print("No extended tracking events found in any records")
        else:
            print(f"Total extended tracking events captured: {total_events}")
        
        # Check expected delivery date calculations
        expected_delivery_count = (df['EXPECTED_DELIVERY_DATE'] != '').sum()
        print(f"Records with expected delivery date calculated: {expected_delivery_count}")
        
        # Check created_pol_atd calculations
        created_pol_atd_count = (df['created_pol_atd'] != '').sum()
        print(f"Records with created_pol_atd populated: {created_pol_atd_count}")
        
    else:
        print("No data retrieved")

if __name__ == "__main__":
    get_tracking_data()