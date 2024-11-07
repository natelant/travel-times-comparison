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
import pandas as pd  
import pytz
from timeseries import timeseries_comparison
from timeseries import summary_table
from time_of_day import temporal_comparison

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

# Route ID input
route_ids_input = st.text_input(
    "Enter Route IDs (comma-separated):",
    help="Example: 13236, 13237"
)



# Convert route IDs string to list of integers
route_ids = [int(id.strip()) for id in route_ids_input.split(',')] if route_ids_input else []

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

# Add a button to trigger the analysis
if st.button("Analyze Data"):
    if not route_ids:
        st.warning("Please enter at least one Route ID")
    else:
        try:
            # Here you'll call your comparison functions
            st.info("Processing data... Please wait.")
            
            # Example calls (commented out until authentication is handled)
            # temporal_data = temporal_comparison(...)
            timeseries_data = timeseries_comparison(
                route_ids,
                window1_start_str,
                window1_end_str,
                window2_start_str,
                window2_end_str,
                username,
                password
            )

            # Get the summary table
            summary_df = summary_table(timeseries_data)
            
            # Display the summary table
            st.subheader("Summary Statistics")
            st.dataframe(summary_df, use_container_width=True)
            
        except Exception as e:
            st.error(f"An error occurred: {str(e)}")

