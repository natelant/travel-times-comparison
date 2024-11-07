import datetime
from api.function import ClearGuideApiHandler
from datetime import datetime, timezone
import pandas as pd  
import pytz
import plotly.graph_objects as go

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()




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

        # Convert minutes to HH:MM format with proper padding
        hours = minutes // 60
        mins = minutes % 60
        time_str = f"{hours:02d}:{mins:02d}"  # Use padding to ensure consistent format
        
        # Add row with statistics and metadata
        data.append([
            route_id,
            dow,
            time_str,
            min_time,
            avg_time,
            max_time
        ])
    
    return pd.DataFrame(data, columns=['route_id', 'day_of_week', 'time', 'min_travel_time', 'avg_travel_time', 'max_travel_time'])

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

def temporal_comparison(route_ids, window1_start_str, window1_end_str, window2_start_str, window2_end_str, username, password):
    # Convert to datetime objects in local time, then convert to UTC
    local_tz = pytz.timezone('America/Denver')  # Salt Lake City uses Mountain Time
    window1_start = local_tz.localize(datetime.strptime(window1_start_str, "%Y-%m-%d %H:%M:%S")).astimezone(timezone.utc)
    window1_end = local_tz.localize(datetime.strptime(window1_end_str, "%Y-%m-%d %H:%M:%S")).astimezone(timezone.utc)
    window2_start = local_tz.localize(datetime.strptime(window2_start_str, "%Y-%m-%d %H:%M:%S")).astimezone(timezone.utc)
    window2_end = local_tz.localize(datetime.strptime(window2_end_str, "%Y-%m-%d %H:%M:%S")).astimezone(timezone.utc)

    # Get raw timeseries data for both periods
    window1_data = get_temporal_data(route_ids, window1_start, window1_end, username, password)
    window2_data = get_temporal_data(route_ids, window2_start, window2_end, username, password)

    # Add period labels to each dataframe
    window1_data['period'] = 'Window 1'
    window2_data['period'] = 'Window 2'

    # Combine the dataframes
    combined_data = pd.concat([window1_data, window2_data], ignore_index=True)
    
    return combined_data

def process_temporal_data(data, selected_days=None, excluded_dates=None):
    # Create a working copy of the data
    filtered_data = data.copy()
    
    # Convert timestamp to day of week
    filtered_data['day_name'] = pd.to_datetime(filtered_data['timestamp']).dt.strftime('%A')
    
    # Filter by selected days if provided
    if selected_days:
        filtered_data = filtered_data[filtered_data['day_name'].isin(selected_days)]
    
    # Filter out excluded dates if provided
    if excluded_dates:
        filtered_data['date'] = pd.to_datetime(filtered_data['timestamp']).dt.strftime('%Y-%m-%d')
        filtered_data = filtered_data[~filtered_data['date'].isin(excluded_dates)]

    # Group data by time and calculate the average for each column
    filtered_data['time'] = pd.to_datetime(filtered_data['timestamp']).dt.strftime('%H:%M')
    data_grouped = filtered_data.groupby(['route_id', 'period','time']).agg({
        'min_travel_time': 'min',
        'avg_travel_time': 'mean',
        'max_travel_time': 'max'
    }).reset_index()
    
    return data_grouped

def build_time_of_day_plot(combined_data):
    fig = go.Figure()
    
    # Create separate traces for Window 1 and Window 2
    for period, color in [('Window 1', 'navy'), ('Window 2', 'blue')]:
        # Create an explicit copy of the filtered data
        period_data = combined_data[combined_data['period'] == period].copy()
        
        # Convert time strings to datetime for proper sorting
        period_data['datetime'] = pd.to_datetime(period_data['time'], format='%H:%M')
        period_data = period_data.sort_values('datetime')
        
        # Add the main line for both windows
        fig.add_trace(go.Scatter(
            x=period_data['time'],
            y=period_data['travel_time_mean'],
            name=period,
            line=dict(color=color),
            mode='lines'
        ))
        
        # Add min/max range only for Window 2
        if period == 'Window 2':
            fig.add_trace(go.Scatter(
                x=period_data['time'],
                y=period_data['travel_time_max'],
                name='Window 2 Range',
                mode='lines',
                line=dict(width=0),
                showlegend=True,
                legendgroup='range',
                fillcolor='rgba(0, 0, 255, 0.2)'
            ))
            fig.add_trace(go.Scatter(
                x=period_data['time'],
                y=period_data['travel_time_min'],
                name='Window 2 Range',
                mode='lines',
                line=dict(width=0),
                fillcolor='rgba(0, 0, 255, 0.2)',
                fill='tonexty',
                showlegend=False,
                legendgroup='range'
            ))
    
    # Update layout
    fig.update_layout(
        title='Travel Time by Time of Day',
        xaxis_title='Time of Day',
        yaxis_title='Travel Time (minutes)',
        showlegend=True,
        xaxis=dict(
            tickformat='%H:%M',
            tickmode='array',
            # Create ticks for every hour (00:00 to 23:00)
            tickvals=[f"{i:02d}:00" for i in range(24)],
            ticktext=[f"{i:02d}:00" for i in range(24)],
            tickangle=45,
            showgrid=True,
            gridwidth=1,
            gridcolor='lightgray'
        )
    )
    
    return fig

# Example usage:
# combined_data = temporal_comparison(
#     route_ids=[13237],
#     window1_start_str="2024-09-01 00:00:00",
#     window1_end_str="2024-09-30 23:59:59",
#     window2_start_str="2024-10-01 00:00:00",
#     window2_end_str="2024-10-31 23:59:59",
#     username=os.getenv('CG_USERNAME'),
#     password=os.getenv('CG_PASSWORD')
# )

# print(combined_data)

# fig = build_time_of_day_plot(combined_data)
# fig.show()


