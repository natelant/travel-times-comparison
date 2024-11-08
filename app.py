# This app will pull data from Clearguide (both the travel times and speed contours APIs) then display in summary tables, charts, and heatmaps.

# Import the necessary libraries
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import datetime
from api.function import ClearGuideApiHandler
from datetime import datetime, timezone
import pytz
from timeseries import timeseries_comparison, build_timeseries_plot, summary_table, process_time_of_day, build_time_of_day_plot
from speed_contours import speed_comparison, process_speed_contours, build_heatmaps


# Set the title and favicon that appear in the Browser's tab bar.
st.set_page_config(
    page_title='Travel Times',
    page_icon='🚙',  # Direct emoji
    # or
    # page_icon='sport_utility_vehicle',  # Shortcode without colons
)

# Add title and logo
st.markdown(
    """
    <div style="display: flex; align-items: center; justify-content: space-between;">
        <img src="https://avenueconsultants.com/wp-content/themes/avenuecustom/webflow/images/a-blue.png" 
             style="width: 100px; margin-left: 20px;">
        <div>
            <h1>Travel Time Comparison</h1>
            <p>Fetching data from Clearguide's travel times and speed contours APIs.</p>
        </div>
        
    </div>
    """, 
    unsafe_allow_html=True
)

# Add some vertical space
st.markdown("<br>", unsafe_allow_html=True)

# Data input section ----------------------------------------------------
st.header('Data Input')
st.write(
    '''
    After generating routes in Clearguide and using the route IDs, select the time windows you want to compare.
    You will also need to upload a KML file that contains the points that represent the intersections on the route for granular segment comparison. 
    '''
)

# Username and password input
username = st.text_input("Enter your Clearguide username:")
password = st.text_input("Enter your Clearguide password:", type="password")

# Route ID input with directions
st.subheader("Route IDs and Directions")
route_info = []
directions = ['Northbound', 'Southbound', 'Eastbound', 'Westbound']

# Create a container for route inputs
route_container = st.container()

with route_container:
    # Route 1
    col1, col2 = st.columns(2)
    with col1:
        route_id1 = st.text_input("Route ID #1", key="route_1")
    with col2:
        direction1 = st.selectbox(
            "Direction #1",
            directions,
            key="direction_1"
        )
    if route_id1:
        route_info.append((route_id1.strip(), direction1))
    
    # Route 2
    col1, col2 = st.columns(2)
    with col1:
        route_id2 = st.text_input("Route ID #2", key="route_2")
    with col2:
        direction2 = st.selectbox(
            "Direction #2",
            directions,
            key="direction_2"
        )
    if route_id2:
        route_info.append((route_id2.strip(), direction2))

# Convert route IDs string to list of integers and store directions
route_ids = [int(rid) for rid, _ in route_info] if route_info else []
route_directions = {int(rid): direction for rid, direction in route_info}

# Create two columns for the time windows
col1, col2 = st.columns(2)

with col1:
    st.subheader("Window 1")
    window1_start = st.date_input("Start Date (Window 1)")
    window1_end = st.date_input("End Date (Window 1)")

with col2:
    st.subheader("Window 2")
    window2_start = st.date_input("Start Date (Window 2)")
    window2_end = st.date_input("End Date (Window 2)")

# Combine dates and times into datetime strings
window1_start_str = f"{window1_start} 00:00:00"
window1_end_str = f"{window1_end} 23:59:59"
window2_start_str = f"{window2_start} 00:00:00"
window2_end_str = f"{window2_end} 23:59:59"

# After the date selection and before the Fetch Data button
st.subheader("KML File Upload")
uploaded_kml = st.file_uploader(
    "Upload KML file containing intersection points",
    type=['kml'],
    help="The KML file is used to plot the intersections on the speed contour heatmaps."
)

# Move the cache function definition outside any button
@st.cache_data
def fetch_timeseries_data(route_ids, window1_start_str, window1_end_str, window2_start_str, window2_end_str, username, password):
    return timeseries_comparison(
        route_ids,
        window1_start_str,
        window1_end_str,
        window2_start_str,
        window2_end_str,
        username,
        password
    )

@st.cache_data
def fetch_speed_contours_data(route_ids, window1_start_str, window1_end_str, window2_start_str, window2_end_str, username, password):
    return speed_comparison(
        route_ids,
        window1_start_str,
        window1_end_str,
        window2_start_str,
        window2_end_str,
        username,
        password
    )
    

# Split into two buttons
if st.button("Fetch Data"):
    if not route_ids:
        st.warning("Please enter at least one Route ID")
    else:
        try:
            # Fetch and cache the data
            st.info("Fetching data... Please wait.")
            st.session_state.timeseries_data = fetch_timeseries_data(
                route_ids,
                window1_start_str,
                window1_end_str,
                window2_start_str,
                window2_end_str,
                username,
                password
            )

            st.session_state.speed_contours_data = fetch_speed_contours_data(
                route_ids,
                window1_start_str,
                window1_end_str,
                window2_start_str,
                window2_end_str,
                username,
                password
            )

            st.success("Data fetched successfully!")
        except Exception as e:
            st.error(f"An error occurred: {str(e)}")

# Add day of week filters
st.subheader("Day of Week Filters")
col1, col2 = st.columns(2)

with col1:
    st.write("Select days to include:")
    days_of_week = {
        'Monday': st.checkbox('Monday', value=True),
        'Tuesday': st.checkbox('Tuesday', value=True),
        'Wednesday': st.checkbox('Wednesday', value=True),
        'Thursday': st.checkbox('Thursday', value=True),
        'Friday': st.checkbox('Friday', value=True),
        'Saturday': st.checkbox('Saturday', value=True),
        'Sunday': st.checkbox('Sunday', value=True)
    }
    selected_days = [day for day, selected in days_of_week.items() if selected]

with col2:
    st.write("Enter dates to exclude (YYYY-MM-DD):")
    excluded_dates_input = st.text_area(
        "One date per line",
        help="Example:\n2024-03-15\n2024-03-16",
        key="excluded_dates"
    )
    excluded_dates = [date.strip() for date in excluded_dates_input.split('\n') if date.strip()]

# Only show analyze button if data exists
if 'timeseries_data' in st.session_state:
    if st.button("Analyze Data"):
        try:
            # Display the summary table
            st.subheader("Summary Statistics")
            # Get the summary table from cached data
            summary_df = summary_table(st.session_state.timeseries_data, selected_days=selected_days, excluded_dates=excluded_dates)
            # Format numbers: integers with no decimals, floats with 2 decimal places
            st.dataframe(
                summary_df.style
                    .format({
                        col: '{:.0f}' if summary_df[col].dtype == 'int64' else '{:.2f}'
                        for col in summary_df.select_dtypes(include=['float64', 'int64']).columns
                    })
                    .applymap(lambda x: 'background-color: #90EE90' if isinstance(x, (int, float)) and x < 0 else '')
                    ,
                use_container_width=True
            )

            # Create columns for each route ID
            num_routes = len(route_ids)
            plot_cols = st.columns(num_routes)

            # Display time of day plots for each route
            #st.subheader("Time of Day Comparison by Route")
            for idx, route_id in enumerate(route_ids):
                with plot_cols[idx]:
                    st.write(f"Route {route_id} - {route_directions[route_id]}")
                    filtered_data = st.session_state.timeseries_data[
                        st.session_state.timeseries_data['route_id'] == route_id
                    ]
                    processed_data = process_time_of_day(filtered_data, selected_days=selected_days, excluded_dates=excluded_dates)
                    
                    # Added unique key for time of day plot
                    st.plotly_chart(
                        build_time_of_day_plot(processed_data),
                        use_container_width=True,
                        key=f"tod_plot_{route_id}"  # Added unique key
                    )

            # Display time series plots for each route
            #st.subheader("Time Series Comparison by Route")
            for idx, route_id in enumerate(route_ids):
                with plot_cols[idx]:
                    st.write(f"Route {route_id} - {route_directions[route_id]}")
                    filtered_data = st.session_state.timeseries_data[
                        st.session_state.timeseries_data['route_id'] == route_id
                    ]
                    # Added unique key for time series plot
                    st.plotly_chart(
                        build_timeseries_plot(filtered_data, selected_days=selected_days, excluded_dates=excluded_dates),
                        use_container_width=True,
                        key=f"ts_plot_{route_id}"  # Added unique key
                    )

            # Display speed contours heatmaps
            for idx, route_id in enumerate(route_ids):
                with plot_cols[idx]:
                    st.write(f"Route {route_id} - {route_directions[route_id]}")
                    filtered_data = st.session_state.speed_contours_data[
                        st.session_state.speed_contours_data['route_id'] == route_id
                    ]

                    processed_data = process_speed_contours(filtered_data, selected_days=selected_days, excluded_dates=excluded_dates)

                    if uploaded_kml is None:
                        st.warning("Please upload a KML file to view the heatmap")
                    else:
                        st.plotly_chart(
                            build_heatmaps(processed_data, uploaded_kml, route_directions[route_id]),
                            use_container_width=True,
                            key=f"heatmap_{route_id}"
                        )

            

            
        except Exception as e:
            st.error(f"An error occurred during analysis: {str(e)}")

