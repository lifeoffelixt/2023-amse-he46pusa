import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

# Load weatherCrashDataNormalized
weatherCrashDataNormalized = pd.read_sql_table('weatherCrashDataNormalized', 'sqlite:///data/data_for_app.sqlite')

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
    color_column = 'Strecke'
    selected_checkboxes = []  # Initialize an empty list for selected checkboxes
else:
    filtered_data = weatherCrashDataNormalized[weatherCrashDataNormalized['Strecke'] == selected_strecke]
    zoom = 5
    color_column = 'NormalizedCrash'
    checkboxes = ['Nebel', 'Black Ice', 'Neuschnee', 'Gesamtschnee', 'Niederschlag', 'Wind', 'Windböen', 'Gesamt', 'NormalizedCrash']
    selected_checkboxes = st.multiselect('Select lines to display', checkboxes, default=['NormalizedCrash'])

# Create scatter plot
fig = px.scatter_mapbox(
    filtered_data,
    lat='Latitude',
    lon='Longitude',
    color=color_column,
    color_continuous_scale='Viridis',
    zoom=zoom,
    hover_data=['Strecke', 'Kilometer', 'NormalizedCrash']
)

# Update map layout
fig.update_layout(
    mapbox_style='carto-positron',
    margin=dict(l=0, r=0, t=0, b=0),
    height=600,  # Set map height
    width=800,  # Set map width
)

# Display map
st.plotly_chart(fig)

# Display graph if a specific Strecke is selected
if selected_strecke != 'All':
    # Create a line graph of selected columns using IDperStrecke as x-axis
    line_data = filtered_data[['Kilometer'] + selected_checkboxes].copy()
    line_data['Kilometer'] = line_data['Kilometer'].astype(str)  # Convert IDperStrecke to string type
    fig_line = go.Figure()

    for checkbox in selected_checkboxes:
        fig_line.add_trace(go.Scatter(x=line_data['Kilometer'], y=line_data[checkbox], name=checkbox, mode='lines', line_shape='spline', showlegend=True))

    # Update the x-axis labels
    fig_line.update_xaxes(title_text='Kilometer')

    # Update the y-axis labels
    fig_line.update_yaxes(title_text='')

    # Update the plot layout
    fig_line.update_layout(height=400, width=800, margin=dict(l=40, r=40, t=40, b=40))  # Set plot height, width, and margins

    # Display the line graph
    st.plotly_chart(fig_line)

    # Define descriptions for each Strecke
    descriptions = {
        'Aschaffenburg_Fuessen': 'On the route Aschaffenburg to Fuessen, there seems to be no trend between the number of crashes and the weather conditions.',
        'Hamburg_Schwieberdingen': 'On the route Hamburg to Schwieberdingen, there seems to be no trend between the number of crashes and the weather conditions.',
        'Karlsruhe_Muenchen': 'On the route Karlsruhe to Muenchen, there seems to be no trend between the number of crashes and the weather conditions.',
        'Koeln_Dresden': 'On the route Koeln to Dresden, there seems to be no trend between the number of crashes and the weather conditions.',
        'Muenchen_Garmisch_Partenkirchen': 'On the route Muenchen to Garmisch Partenkirchen, there seems to be no trend between the number of crashes and the weather conditions.',
        'Muenchen_Nuernberg': 'On the route Muenchen to Nuernberg, there seems to be no trend between the number of crashes and the weather conditions.',
        'Muenchen_Salzburg': 'On the route Muenchen to Salzburg, there seems to be no trend between the number of crashes and the weather conditions.',
        'Nuernberg_Suhl': 'On the route Nuernberg to Suhl, there seems to be no trend between the number of crashes and the weather conditions.',
        'Wuerzburg_Berlin': 'On the route Wuerzburg to Berlin, there seems to be no trend between the number of crashes and the weather conditions.',
        'Wuppertal_Kassel': 'On the route Wuppertal to Kassel, there seems to be no trend between the number of crashes and the weather conditions.'
    }

    # Display description based on the selected Strecke
    if selected_strecke in descriptions:
        st.markdown("Findings:")
        st.write(descriptions[selected_strecke])
