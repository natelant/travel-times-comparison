# This app will pull data from Clearguide (both the travel times and speed contours APIs) then display in summary tables, charts, and heatmaps.

# Import the necessary libraries
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import requests
import json
import time
import os

# Set the title and favicon that appear in the Browser's tab bar.
st.set_page_config(
    page_title='Travel Times',
    page_icon=':sport_utility_vehicle:',
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

# File upload section
st.header('Data Input')
st.write(
    '''
    After generating routes in Clearguide, select the times and ranges you want to compare.
    You will also need to upload a KML file that contains the points that represent the intersections on the route for granular segment comparison. 
    '''
)