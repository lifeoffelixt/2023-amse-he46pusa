import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

# Load weatherCrashDataNormalized
weatherCrashDataNormalized = pd.read_sql_table('weatherCrashDataNormalized', 'sqlite:///data/data.sqlite')

# Get unique values of Strecke
strecke_values = weatherCrashDataNormalized['Strecke'].unique()

# Set title
st.title("Weather Crash Data Heatmap")

# Create dropdown chooser
selected_strecke = st.selectbox('Select Strecke', ['All'] + list(strecke_values))

# Filter the data for the selected Strecke
if selected_strecke == 'All':
    filtered_data = weatherCrashDataNormalized
    zoom = 4.5
else:
    filtered_data = weatherCrashDataNormalized[weatherCrashDataNormalized['Strecke'] == selected_strecke]
    zoom = 5

# Create checkboxes for enabling/disabling lines
checkboxes = ['NormalizedCrash', 'Nebel', 'Black Ice', 'Neuschnee', 'Gesamtschnee', 'Niederschlag', 'Wind', 'Windböen', 'Gesamt']
selected_checkboxes = st.multiselect('Select lines to display', checkboxes, default=['NormalizedCrash'])

# Create scatter plot
fig = px.scatter_mapbox(
    filtered_data,
    lat='Latitude',
    lon='Longitude',
    color='NormalizedCrash',
    color_continuous_scale='Viridis',
    zoom=zoom,
    hover_data=['Strecke'],
)

# Update map layout
fig.update_layout(
    mapbox_style='carto-positron',
    margin=dict(l=0, r=0, t=0, b=0),
)

# Display map
st.plotly_chart(fig)

# Display graph if a specific Strecke is selected
if selected_strecke != 'All':
    # Create a line graph of selected columns using IDperStrecke as x-axis
    line_data = filtered_data[['IDperStrecke'] + selected_checkboxes].copy()
    line_data['IDperStrecke'] = line_data['IDperStrecke'].astype(str)  # Convert IDperStrecke to string type
    fig_line = go.Figure()

    for checkbox in selected_checkboxes:
        fig_line.add_trace(go.Scatter(x=line_data['IDperStrecke'], y=line_data[checkbox], name=checkbox, mode='lines', line_shape='spline'))

    # Update the x-axis labels
    fig_line.update_xaxes(title_text='Kilometer')

    # Update the y-axis labels
    fig_line.update_yaxes(title_text='')

    # Display the line graph
    st.plotly_chart(fig_line)
