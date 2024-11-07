import datetime
from api.function import ClearGuideApiHandler
from datetime import datetime, timezone
import pandas as pd  
import pytz
from scipy import stats




# Functions -----------------------------------------------------------------
#-------------------------------------------------------------------------
def parse_timeseries_json_response(response, route_id):
    data = []
    
    # Extract the data array
    data_array = response['series']['all']['avg_travel_time']['data']
    
    for entry in data_array:
        timestamp = entry[0]  # Unix timestamp
        travel_time = entry[1]
        
        
            
        # Convert Unix timestamp to datetime (UTC) and then to local time
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        local_dt = dt.astimezone(pytz.timezone('America/Denver'))
        formatted_time = local_dt.strftime('%Y-%m-%d %H:%M:%S')
        
        data.append([route_id, formatted_time, travel_time])
    
    # Convert to DataFrame with column names
    columns = ['route_id', 'timestamp', 'travel_time']
    return pd.DataFrame(data, columns=columns)

def get_timeseries_data(route_ids, start_datetime, end_datetime, username, password):
    # Define the API parameters
    API_URL = 'https://api.iteris-clearguide.com/v1/route/timeseries/'
    CUSTOMER_KEY = 'ut'
    ROUTE_ID_TYPE = 'customer_route_number'
    # Use datetime strings directly (YYYY-MM-DD HH:MM:SS format)
    START_TIMESTAMP = start_datetime.strftime("%Y-%m-%d %H:%M:%S")
    END_TIMESTAMP = end_datetime.strftime("%Y-%m-%d %H:%M:%S")
    METRIC = 'avg_travel_time'
    GRANULARITY = '5min'
    INCLUDE_HOLIDAYS = 'false'

    cg_api_handler = ClearGuideApiHandler(username=username, password=password)
    all_parsed_data = []  # Initialize empty list to store all route data

    try:
        for route_id in route_ids:
            query = f'{API_URL}?customer_key={CUSTOMER_KEY}&route_id={route_id}&route_id_type={ROUTE_ID_TYPE}&s_timestamp={START_TIMESTAMP}&e_timestamp={END_TIMESTAMP}&metrics={METRIC}&holidays={INCLUDE_HOLIDAYS}&granularity={GRANULARITY}'
            
            response = cg_api_handler.call(url=query)
            
            if 'error' in response and response['error']:
                raise Exception(f"Error fetching response for route_id {route_id}... Message: {response.get('msg', 'No message provided')}")
            
            # Parse JSON response and append to all_parsed_data
            route_data = parse_timeseries_json_response(response, route_id)
            all_parsed_data.append(route_data)
            
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise

    # Combine all route data into a single DataFrame
    return pd.concat(all_parsed_data, ignore_index=True)

def timeseries_comparison(route_ids, window1_start, window1_end, window2_start, window2_end, username, password):
    # Convert both windows to datetime objects
    local_tz = pytz.timezone('America/Denver')
    windows = {
        'window1': (
            local_tz.localize(datetime.strptime(window1_start, "%Y-%m-%d %H:%M:%S")).astimezone(timezone.utc),
            local_tz.localize(datetime.strptime(window1_end, "%Y-%m-%d %H:%M:%S")).astimezone(timezone.utc)
        ),
        'window2': (
            local_tz.localize(datetime.strptime(window2_start, "%Y-%m-%d %H:%M:%S")).astimezone(timezone.utc),
            local_tz.localize(datetime.strptime(window2_end, "%Y-%m-%d %H:%M:%S")).astimezone(timezone.utc)
        )
    }

    # Fetch data for both windows
    data_window1 = get_timeseries_data(route_ids, windows['window1'][0], windows['window1'][1], username, password)
    data_window2 = get_timeseries_data(route_ids, windows['window2'][0], windows['window2'][1], username, password)

    # Add period column to each dataset
    data_window1['period'] = 'window1'
    data_window2['period'] = 'window2'

    # Combine the datasets
    combined_data = pd.concat([data_window1, data_window2], ignore_index=True)
    return combined_data

# Example usage:
# combined_data = timeseries_comparison(
#     route_ids=[13236, 13237],
#     window1_start="2024-09-01 00:00:00",
#     window1_end="2024-09-30 23:59:59",
#     window2_start="2024-10-01 00:00:00",
#     window2_end="2024-10-31 23:59:59",

# )

def summary_table(combined_data):

    # Filter goes here...

    # Group by route_id and period, calculate summary statistics
    summary = combined_data.groupby(['route_id', 'period'])['travel_time'].agg([
        ('mean', 'mean'),
        ('n', 'count')  # Add count to get sample size
    ]).round(2)
    
    # Pivot the table to have windows as columns
    summary_pivoted = summary.unstack(level='period')
    
    # Calculate the difference and percent change
    summary_pivoted['diff'] = summary_pivoted[('mean', 'window2')] - summary_pivoted[('mean', 'window1')]
    summary_pivoted['pct_change'] = (summary_pivoted['diff'] / summary_pivoted[('mean', 'window1')]) * 100

    # Calculate p-value for each route using t-test
    def calculate_pvalue(route_id):
        window1_data = combined_data[(combined_data['route_id'] == route_id) & 
                                   (combined_data['period'] == 'window1')]['travel_time']
        window2_data = combined_data[(combined_data['route_id'] == route_id) & 
                                   (combined_data['period'] == 'window2')]['travel_time']
        _, p_value = stats.ttest_ind(window1_data, window2_data)
        # Format very small p-values
        return f'{p_value:.4e}' if p_value < 0.0001 else f'{p_value:.4f}'

    # Add p-values to the summary table
    summary_pivoted['p_value'] = summary_pivoted.index.map(calculate_pvalue)

    # Flatten column names
    summary_pivoted.columns = [f'{window}_{stat}' 
                             for window, stat in summary_pivoted.columns]

    # Reset index to make route_id a regular column
    return summary_pivoted.reset_index()

print(summary_table(combined_data))

