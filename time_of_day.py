import datetime
from api.function import ClearGuideApiHandler
import json
from datetime import datetime, timezone
import pandas as pd  
import pytz




# Functions -----------------------------------------------------------------
#-------------------------------------------------------------------------
def parse_temporal_json_response(response, route_id):
    data = []
    
    # Extract the data array
    data_array = response['series']['all']['avg_travel_time']['data']
    
    for entry in data_array:
        dow_data = entry[0]  # Day of week (e.g., 'sun')
        stats = entry[1]  # Array of [min, avg, max] travel times
        
        # Unpack statistics
        dow, minutes = dow_data
        min_time, avg_time, max_time = stats

        # Convert minutes to HH:MM format
        time_str = f"{minutes // 60}:{minutes % 60:02d}"
        
        # Add row with statistics and metadata
        data.append([
            route_id,
            dow,
            time_str,
            min_time,
            avg_time,
            max_time
        ])
    
    # Update columns to reflect new data structure
    columns = ['route_id', 'day_of_week', 'time', 'min_travel_time', 'avg_travel_time', 'max_travel_time']
    return pd.DataFrame(data, columns=columns)

def get_temporal_data(route_ids, start_datetime, end_datetime, username, password):
    # Define the API parameters
    API_URL = 'https://api.iteris-clearguide.com/v1/route/temporal/'
    CUSTOMER_KEY = 'ut'
    ROUTE_ID_TYPE = 'customer_route_number'
    # Use datetime strings directly (YYYY-MM-DD HH:MM:SS format)
    START_TIMESTAMP = start_datetime.strftime("%Y-%m-%d %H:%M:%S")
    END_TIMESTAMP = end_datetime.strftime("%Y-%m-%d %H:%M:%S")
    METRIC = 'avg_travel_time'
    GRANULARITY = '5min'
    INCLUDE_HOLIDAYS = 'false'
    DOW = 'true'
    TOD = 'true'
    STATISTICS = 'min,avg,max'

    cg_api_handler = ClearGuideApiHandler(username=username, password=password)
    all_parsed_data = []  # Initialize empty list to store all route data

    try:
        for route_id in route_ids:
            query = f'{API_URL}?customer_key={CUSTOMER_KEY}&route_id={route_id}&route_id_type={ROUTE_ID_TYPE}&s_timestamp={START_TIMESTAMP}&e_timestamp={END_TIMESTAMP}&metrics={METRIC}&holidays={INCLUDE_HOLIDAYS}&granularity={GRANULARITY}&dow={DOW}&tod={TOD}&statistics={STATISTICS}'
            
            response = cg_api_handler.call(url=query)
            
            if 'error' in response and response['error']:
                raise Exception(f"Error fetching response for route_id {route_id}... Message: {response.get('msg', 'No message provided')}")
            
            # Parse JSON response and append to all_parsed_data
            route_data = parse_temporal_json_response(response, route_id)
            all_parsed_data.append(route_data)
            
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise

    # Combine all route data into a single DataFrame
    return pd.concat(all_parsed_data, ignore_index=True)


# Inputs -------------------------------------------------------------------


route_ids = [13236]

# Data download window (YYYY-MM-DD HH:MM:SS)
start_datetime_str = "2024-09-1 00:00:00"
end_datetime_str = "2024-10-31 23:59:59"

# Convert to datetime objects in local time, then convert to UTC
local_tz = pytz.timezone('America/Denver')  # Salt Lake City uses Mountain Time
start_datetime = local_tz.localize(datetime.strptime(start_datetime_str, "%Y-%m-%d %H:%M:%S")).astimezone(timezone.utc)
end_datetime = local_tz.localize(datetime.strptime(end_datetime_str, "%Y-%m-%d %H:%M:%S")).astimezone(timezone.utc)

# Get timeseries data
data = get_temporal_data(route_ids, start_datetime, end_datetime, username, password)

def process_temporal_data(data):

    # Filter out days of the week...
    data = data[data['day_of_week'] != 'sat']
    data = data[data['day_of_week'] != 'sun']

    # group data by time and calculate the average for each column
    data_grouped = data.groupby('time').agg({
        'min_travel_time': 'min',
        'avg_travel_time': 'mean',
        'max_travel_time': 'max'
    }).reset_index()

    return data_grouped

data_grouped = process_temporal_data(data)
# print the data
print(data_grouped)