import datetime
from api.function import ClearGuideApiHandler
from datetime import datetime, timezone
import pandas as pd  
import pytz
from scipy import stats
import os
from dotenv import load_dotenv
import plotly.graph_objects as go
from geopy.distance import geodesic
from pykml import parser

# Load environment variables
load_dotenv()

# Functions -----------------------------------------------------------------
#-------------------------------------------------------------------------
def parse_json_response(response, route_id):

    data = []
    
    # Extract the data array
    data_array = response['series']['all']['avg_speed']['data']
    
    for entry in data_array:
        timestamp = entry[0]  # Unix timestamp
        measurements = entry[1]
        
        for measurement in measurements:
            distance = measurement[0]
            speed = measurement[1]
            
            # Convert Unix timestamp to datetime (UTC) and then to local time
            dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            local_dt = dt.astimezone(pytz.timezone('America/Denver'))
            formatted_time = local_dt.strftime('%Y-%m-%d %H:%M:%S')
            
            data.append([route_id, formatted_time, distance, speed])
    
    return data

def calculate_distances(intersections, direction):
    if direction in ['Northbound', 'Eastbound']:
        start_point = intersections[-1]  # Last intersection (northernmost or easternmost)
    else:
        start_point = intersections[0]  # First intersection (southernmost or westernmost)
    
    distances = []
    for intersection in intersections:
        distance_miles = geodesic((start_point[1], start_point[2]), (intersection[1], intersection[2])).miles
        distances.append((intersection[0], round(distance_miles, 2)))
    
    return distances

# Read in a KML file for the y axis of the heatmap
def read_kml_intersections(kml_file_path):
    with open(kml_file_path, 'rb') as kml_file:
        root = parser.parse(kml_file).getroot()
        placemarks = root.findall('.//{http://www.opengis.net/kml/2.2}Placemark')
        intersections = []
        for placemark in placemarks:
            name = placemark.find('{http://www.opengis.net/kml/2.2}name').text
            coordinates = placemark.find('.//{http://www.opengis.net/kml/2.2}coordinates').text.split(',')
            lat, lon = float(coordinates[1]), float(coordinates[0])
            intersections.append((name, lat, lon))
    return intersections

def get_speed_data(route_ids, start_datetime, end_datetime, username, password):
    # Define the API parameters
    API_URL = 'https://api.iteris-clearguide.com/v1/route/spatial/contours/'
    CUSTOMER_KEY = 'ut'
    ROUTE_ID_TYPE = 'customer_route_number'
    # Use datetime strings directly (YYYY-MM-DD HH:MM:SS format)
    START_TIMESTAMP = start_datetime.strftime("%Y-%m-%d %H:%M:%S")
    END_TIMESTAMP = end_datetime.strftime("%Y-%m-%d %H:%M:%S")
    METRIC = 'avg_speed'
    GRANULARITY = 'hour'
    INCLUDE_HOLIDAYS = 'false'

    cg_api_handler = ClearGuideApiHandler(username=username, password=password)
    all_parsed_data = []

    try:
        for route_id in route_ids:
            query = f'{API_URL}?customer_key={CUSTOMER_KEY}&route_id={route_id}&route_id_type={ROUTE_ID_TYPE}&s_timestamp={START_TIMESTAMP}&e_timestamp={END_TIMESTAMP}&metrics={METRIC}&holidays={INCLUDE_HOLIDAYS}&granularity={GRANULARITY}'

            response = cg_api_handler.call(url=query)

            if 'error' in response and response['error']:
                raise Exception(f"Error fetching response for route_id {route_id}... Message: {response.get('msg', 'No message provided')}")

            # Parse JSON response and append to all_parsed_data
            route_data = parse_json_response(response, route_id)
            # Convert list of lists to DataFrame before appending
            df = pd.DataFrame(route_data, columns=['route_id', 'timestamp', 'distance', 'speed'])
            all_parsed_data.append(df)
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise

    # Combine all route data into a single DataFrame
    return pd.concat(all_parsed_data, ignore_index=True)
    
def speed_comparison(route_ids, window1_start, window1_end, window2_start, window2_end, username, password):
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
    data_window1 = get_speed_data(route_ids, windows['window1'][0], windows['window1'][1], username, password)
    data_window2 = get_speed_data(route_ids, windows['window2'][0], windows['window2'][1], username, password)

    # Add period column to each dataset
    data_window1['period'] = 'window1'
    data_window2['period'] = 'window2'

    # Combine the datasets
    combined_data = pd.concat([data_window1, data_window2], ignore_index=True)
    return combined_data

def process_speed_contours(combined_data, selected_days, excluded_dates): # assume that a single route is being processed here
    # Convert timestamp to datetime first
    combined_data['timestamp'] = pd.to_datetime(combined_data['timestamp'])

    # Filter excluded dates
    combined_data['date'] = combined_data['timestamp'].dt.strftime('%Y-%m-%d')
    combined_data = combined_data[~combined_data['date'].isin(excluded_dates)]

    # Filter selected days
    combined_data['day_name'] = combined_data['timestamp'].dt.strftime('%A')
    combined_data = combined_data[combined_data['day_name'].isin(selected_days)]

    # Create a function to bin distances
    def bin_distance(distance):
        return round(distance, 2)  # Round to 2 decimal places for binning

    # Add hour and binned distance columns
    combined_data['hour'] = combined_data['timestamp'].dt.hour
    combined_data['binned_distance'] = combined_data['distance'].apply(bin_distance)

    # Group by period, hour, binned distance, and calculate the mean speed
    summary = combined_data.groupby(['period', 'hour', 'binned_distance'])['speed'].mean().reset_index()

    # Pivot the table to have periods as columns
    summary_pivoted = summary.pivot(index=['hour', 'binned_distance'], columns='period', values='speed').reset_index() # NOT GROUPING BY ROUTE_ID assume that a single route is being processed here

    # Calculate the difference and percent change
    summary_pivoted['diff'] = summary_pivoted['window2'] - summary_pivoted['window1']
    summary_pivoted['percent_change'] = (summary_pivoted['diff'] / summary_pivoted['window1']) * 100

    return summary_pivoted


def build_heatmaps(summary_pivoted, kml_file_path, direction):
    # Plotting the speed difference in a heatmap using Plotly
    intersections = read_kml_intersections(kml_file_path)

    # Calculate distances based on direction
    intersection_distances = calculate_distances(intersections, direction) # Direction needs to be Northbound or Southbound or Eastbound or Westbound

    # Create a pivot table for the heatmap
    pivot_data = summary_pivoted.pivot(index='binned_distance', columns='hour', values='diff')

    # Sort the pivot_data index to ensure it's in ascending order
    pivot_data = pivot_data.sort_index()
    
    # Prepare y-axis labels
    y_ticks = [dist for _, dist in intersection_distances]
    y_labels = [f"{name} ({dist:.2f} mi)" for name, dist in intersection_distances]
    
    # Create the heatmap using Plotly
    fig = go.Figure(data=go.Heatmap(
        z=pivot_data.values,
        x=pivot_data.columns,
        y=pivot_data.index,
        colorscale='RdYlGn',
        zmid=0,
        colorbar=dict(title='Speed Difference (mph)')
    ))

    # Update layout
    fig.update_layout(
        title=f"Speed Difference Heatmap - {direction}",
        xaxis_title="Hour of Day",
        yaxis_title="Distance (miles)"
        # height=1000,  # Adjust the height as needed
        # width=2000,    # Adjust the width as needed
    )

    # Customize y-axis ticks and labels
    fig.update_yaxes(
        tickmode='array',
        tickvals=y_ticks,
        ticktext=y_labels,
        autorange='reversed'  # This inverts the y-axis
    )

    return fig


# -------------------------------------------------
# Example usage:

# example_data = speed_comparison(
#     route_ids=[13236], 
#     window1_start="2024-09-01 00:00:00", 
#     window1_end="2024-09-30 23:59:59", 
#     window2_start="2024-10-01 00:00:00", 
#     window2_end="2024-10-31 23:59:59", 
#     username=os.getenv('CG_USERNAME'), 
#     password=os.getenv('CG_PASSWORD')
# )

# processed = process_speed_contours(example_data, selected_days=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'], excluded_dates=[])

# fig = build_heatmaps(processed, 'sample_data/State Street (9000 South to 11400 South).kml', 'Southbound')
# fig.show()


