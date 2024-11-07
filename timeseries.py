import datetime
from api.function import ClearGuideApiHandler
from datetime import datetime, timezone
import pandas as pd  
import pytz
from scipy import stats
import os
from dotenv import load_dotenv
import plotly.graph_objects as go

# Load environment variables
load_dotenv()

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

def summary_table(combined_data, selected_days, excluded_dates):

    # Filter excluded dates
    combined_data['date'] = pd.to_datetime(combined_data['timestamp']).dt.strftime('%Y-%m-%d')
    combined_data = combined_data[~combined_data['date'].isin(excluded_dates)]

    # Filter selected days
    combined_data['day_name'] = pd.to_datetime(combined_data['timestamp']).dt.strftime('%A')
    combined_data = combined_data[combined_data['day_name'].isin(selected_days)]

    # Group by route_id and period, calculate summary statistics
    summary = combined_data.groupby(['route_id', 'period'])['travel_time'].agg([
        ('mean', 'mean'),
        ('n', 'count')  # Add count to get sample size
    ]).round(2)
    
    # Pivot the table to have windows as columns
    summary_pivoted = summary.unstack(level='period')
    
    # Flatten column names
    summary_pivoted.columns = [f'{col[1]}_{col[0]}' if isinstance(col, tuple) else col 
                             for col in summary_pivoted.columns]
    
    # Reset index to make route_id a regular column
    summary_pivoted = summary_pivoted.reset_index()

    # Calculate the difference and percent change
    summary_pivoted['diff'] = summary_pivoted['window2_mean'] - summary_pivoted['window1_mean']
    summary_pivoted['pct_change'] = (summary_pivoted['diff'] / summary_pivoted['window1_mean']) * 100

    # Calculate p-values
    def calculate_pvalue(route_id):
        window1_data = combined_data[(combined_data['route_id'] == route_id) & 
                                   (combined_data['period'] == 'window1')]['travel_time']
        window2_data = combined_data[(combined_data['route_id'] == route_id) & 
                                   (combined_data['period'] == 'window2')]['travel_time']
        _, p_value = stats.ttest_ind(window1_data, window2_data)
        return f'{p_value:.4e}' if p_value < 0.0001 else f'{p_value:.4f}'

    # Add p-values to the summary table
    summary_pivoted['p_value'] = summary_pivoted['route_id'].map(calculate_pvalue)

    # Create a mapping for prettier column names
    column_mapping = {
        'route_id': 'Route ID',
        'window1_mean': 'Mean (Window 1)',
        'window2_mean': 'Mean (Window 2)',
        'diff': 'Change (Minutes)',
        'pct_change': '% Change',
        'p_value': 'P-Value',
        'window1_n': 'Sample Size (Window 1)',
        'window2_n': 'Sample Size (Window 2)',
    }
    
    # Define the desired column order
    column_order = ['route_id', 'window1_mean', 'window2_mean', 'diff', 'pct_change', 'p_value', 'window1_n', 'window2_n']
    
    # Reorder and rename columns
    summary_pivoted = summary_pivoted[column_order]
    summary_pivoted.columns = [column_mapping[col] for col in column_order]

    return summary_pivoted

def build_timeseries_plot(combined_data, selected_days, excluded_dates):

    # Filter excluded dates
    combined_data['date'] = pd.to_datetime(combined_data['timestamp']).dt.strftime('%Y-%m-%d')
    combined_data = combined_data[~combined_data['date'].isin(excluded_dates)]

    # Filter selected days
    combined_data['day_name'] = pd.to_datetime(combined_data['timestamp']).dt.strftime('%A')
    combined_data = combined_data[combined_data['day_name'].isin(selected_days)]

    fig = go.Figure()
    
    # Add traces for each window
    fig.add_trace(go.Scatter(
        x=combined_data[combined_data['period'] == 'window1']['timestamp'],
        y=combined_data[combined_data['period'] == 'window1']['travel_time'],
        mode='markers',
        name='Window 1',
        marker=dict(color='navy', size=6)
    ))
    
    fig.add_trace(go.Scatter(
        x=combined_data[combined_data['period'] == 'window2']['timestamp'],
        y=combined_data[combined_data['period'] == 'window2']['travel_time'],
        mode='markers',
        name='Window 2',
        marker=dict(color='blue', size=6)
    ))

    # Calculate and add mean lines for each window
    window1_mean = combined_data[combined_data['period'] == 'window1']['travel_time'].mean()
    window2_mean = combined_data[combined_data['period'] == 'window2']['travel_time'].mean()

    # Add horizontal mean lines
    fig.add_hline(y=window1_mean, line_dash="dash", line_color="navy", 
                 annotation_text=f"Window 1 Mean: {window1_mean:.2f}")
    fig.add_hline(y=window2_mean, line_dash="dash", line_color="red", 
                 annotation_text=f"Window 2 Mean: {window2_mean:.2f}")
    
    # Update layout
    fig.update_layout(
        title='Timeseries Comparison',
        xaxis_title='Timestamp',
        yaxis_title='Travel Time (minutes)',
        showlegend=True,
        template='plotly_white'
    )
    
    return fig


def process_time_of_day(combined_data, selected_days, excluded_dates):
    # Filter excluded dates
    combined_data['date'] = pd.to_datetime(combined_data['timestamp']).dt.strftime('%Y-%m-%d')
    combined_data = combined_data[~combined_data['date'].isin(excluded_dates)]
    # Filter selected_days
    combined_data['day_name'] = pd.to_datetime(combined_data['timestamp']).dt.strftime('%A')
    combined_data = combined_data[combined_data['day_name'].isin(selected_days)]

    combined_data['day_of_week'] = pd.to_datetime(combined_data['timestamp']).dt.strftime('%w')
    combined_data['time'] = pd.to_datetime(combined_data['timestamp']).dt.strftime('%H:%M')

    # Group by route_id, period, NOT day_of_week, and time and create columns for min, avg, and max travel time 
    data_grouped = combined_data.groupby(['route_id', 'period', 'time']).agg({
        'travel_time': ['min', 'mean', 'max']
    }).reset_index()
    
    # Flatten column names
    data_grouped.columns = ['route_id', 'period', 'time', 'travel_time_min', 'travel_time_mean', 'travel_time_max']

    return data_grouped


def build_time_of_day_plot(combined_data):
    fig = go.Figure()
    
    # Create separate traces for Window 1 and Window 2
    for period, color in [('window1', 'navy'), ('window2', 'blue')]:
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
        if period == 'window2':
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


# -------------------------------------------------------------------------
# Example usage:


# combined_data = timeseries_comparison(
#     route_ids=[13236],
#     window1_start="2024-09-01 00:00:00",
#     window1_end="2024-09-30 23:59:59",
#     window2_start="2024-10-01 00:00:00",
#     window2_end="2024-10-31 23:59:59",
#     username=os.getenv('CG_USERNAME'),
#     password=os.getenv('CG_PASSWORD')
# )

# processed = process_time_of_day(combined_data, selected_days=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'], excluded_dates=[])

# print(summary_table(combined_data, selected_days=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'], excluded_dates=[]))
# # # show the plot
# fig = build_time_of_day_plot(processed)
# fig.show()

