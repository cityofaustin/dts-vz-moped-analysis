# %%
# Package imports
import pandas as pd
import geopandas as gpd
import plotly.express as px  
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, dash_table 
import dash_bootstrap_components as dbc
from shapely.geometry import MultiLineString, LineString
import plotly.graph_objs as go
import dash_leaflet as dl

# %%
# Data imports
vz_moped = pd.read_csv('../Output/annualized_statistics.csv')

# %%
# Setting the line_geometry as the geometry column using geopandas
vz_moped['line_geometry'] = gpd.GeoSeries.from_wkt(vz_moped['line_geometry'])

# %%
# Data manipulation

# Changing null values to "N/As"
vz_moped['component_subtype'] = vz_moped['component_subtype'].fillna("N/A")
vz_moped['component_work_types'] = vz_moped['component_work_types'].fillna("N/A")

# Converting to datetime
vz_moped['substantial_completion_date'] = pd.to_datetime(vz_moped['substantial_completion_date'])

# Creating completion year variable
vz_moped['completion_year'] = vz_moped['substantial_completion_date'].dt.year

# Flag for involving fatality
# 1 if pre/post fatal crash rate is not null
vz_moped['component_had_fatal_crash'] = (vz_moped['pre_annualized_fatal_crash_rate'] > 0) | (vz_moped['post_annualized_fatal_crash_rate'] > 0)
vz_moped['component_had_fatal_crash'] = vz_moped['component_had_fatal_crash'].apply(lambda x: "Yes" if x else "No")

# %%
vz_moped

# %%
vz_moped.info()

# %%
# Data that will be plotted

# Table
columns_to_display = [
    'component_name', 'component_name_full',
    'component_subtype', 'component_work_types', 'type_name',
    'substantial_completion_date',
    'pre_annualized_crash_rate', 'post_annualized_crash_rate',
    'delta_crash_rate', 'pre_annualized_fatal_crash_rate',
    'post_annualized_fatal_crash_rate', 'delta_fatal_crash_rate',
    'pre_annualized_injury_rate', 'post_annualized_injury_rate',
    'delta_injury_rate', 'pre_annualized_death_rate',
    'post_annualized_death_rate', 'delta_death_rate', 'pre_annualized_cost',
    'post_annualized_cost', 'delta_comp_cost',
    'component_had_fatal_crash'
]

column_labels = {
    'moped_component_id': 'Component ID',
    'component_name': 'Name',
    'component_name_full': 'Full Name',
    'component_subtype': 'Subtype',
    'component_work_types': 'Work Types',
    'type_name': 'Type',
    'substantial_completion_date': 'Completion Date',
    'pre_annualized_crash_rate': 'Pre Annualized Crash Rate',
    'post_annualized_crash_rate': 'Post Annualized Crash Rate',
    'delta_crash_rate': 'Delta Crash Rate',
    'pre_annualized_fatal_crash_rate': 'Pre Annualized Fatal Crash Rate',
    'post_annualized_fatal_crash_rate': 'Post Annualized Fatal Crash Rate',
    'delta_fatal_crash_rate': 'Delta Fatal Crash Rate',
    'pre_annualized_injury_rate': 'Pre Annualized Injury Rate',
    'post_annualized_injury_rate': 'Post Annualized Injury Rate',
    'delta_injury_rate': 'Delta Injury Rate',
    'pre_annualized_death_rate': 'Pre Annualized Death Rate',
    'post_annualized_death_rate': 'Post Annualized Death Rate',
    'delta_death_rate': 'Delta Death Rate',
    'pre_annualized_cost': 'Pre Annualized Cost',
    'post_annualized_cost': 'Post Annualized Cost',
    'delta_comp_cost': 'Delta Completion Cost',
    'component_had_fatal_crash': 'Had Fatal Crash'
}

# %%
# Creating prototype elements

# card
def create_card(title, dropdown_id, dropdown_options):
    return dbc.Card(
        [
            dbc.CardBody(
                [
                    html.H6(title, className="card_title"),
                    dcc.Dropdown(
                        id=dropdown_id,
                        options=[{"label": option, "value": option} for option in dropdown_options],
                        placeholder="Select"
                    )
                ],
                className="custom_card_body"
            )
        ],
        className="custom_card"
    )

# %%
app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Simplify the geometries for performance
SIMPLIFY_TOLERANCE = 0.01

def create_geojson(filtered_df):
    if filtered_df.empty:
        return {}
    
    # Convert to GeoDataFrame for easier manipulation
    gdf = gpd.GeoDataFrame(filtered_df, geometry='line_geometry')

    # Simplify geometries
    gdf['line_geometry'] = gdf['line_geometry'].apply(lambda geom: geom.simplify(SIMPLIFY_TOLERANCE))

    # Convert all Timestamp columns to string
    for column in gdf.select_dtypes(include=['datetimetz', 'datetime64']).columns:
        gdf[column] = gdf[column].astype(str)
    
    # Convert to GeoJSON format
    geojson_data = gdf.to_json()

    return geojson_data

app.layout = dbc.Container([
    dbc.Row(
        dbc.Col(html.H1("Map Visualization", className="app_title"),
                width=12),
        className="justify-content-center"
    ),
    dbc.Row(
        [
            dbc.Col(
                create_card("Completion year", "dropdown_year", sorted(vz_moped['completion_year'].unique())),
                className="navigation_row_col_style"
            ),
            dbc.Col(
                create_card("Component name", "dropdown_component_name", vz_moped['component_name'].unique()),
                className="navigation_row_col_style"
            ),
            dbc.Col(
                create_card("Component sub-type", "dropdown_component_subtype", vz_moped['component_subtype'].unique()),
                className="navigation_row_col_style"
            ),
            dbc.Col(
                create_card("Component work-type", "dropdown_work_type", vz_moped['component_work_types'].unique()),
                className="navigation_row_col_style"
            ),
            dbc.Col(
                create_card("Fatal crash", "dropdown_fatal_crash", vz_moped['component_had_fatal_crash'].unique()),
                className="navigation_row_col_style"
            )
        ],
        className="navigation_row"
    ),
    dbc.Row(
        dbc.Col(
            dl.Map(center=[30.2672, -97.7431], zoom=10, children=[
                dl.TileLayer(),
                dl.GeoJSON(id='geojson')  # This will be the layer for your geometries
            ]),
            width=12
        )
    )
], fluid=True, className="custom_container")

@app.callback(
    Output('geojson', 'data'),
    [
        Input('dropdown_year', 'value'),
        Input('dropdown_component_name', 'value'),
        Input('dropdown_component_subtype', 'value'),
        Input('dropdown_work_type', 'value'),
        Input('dropdown_fatal_crash', 'value')
    ]
)
def update_map(dropdown_year, dropdown_component_name, dropdown_component_subtype,
               dropdown_work_type, dropdown_fatal_crash):
    # Base DataFrame
    filtered_df = vz_moped

    # Convert single values to list to avoid TypeError
    if dropdown_year and not isinstance(dropdown_year, list):
        dropdown_year = [dropdown_year]
    if dropdown_component_name and not isinstance(dropdown_component_name, list):
        dropdown_component_name = [dropdown_component_name]
    if dropdown_component_subtype and not isinstance(dropdown_component_subtype, list):
        dropdown_component_subtype = [dropdown_component_subtype]
    if dropdown_work_type and not isinstance(dropdown_work_type, list):
        dropdown_work_type = [dropdown_work_type]
    if dropdown_fatal_crash and not isinstance(dropdown_fatal_crash, list):
        dropdown_fatal_crash = [dropdown_fatal_crash]

    # Apply filters
    if dropdown_year:
        filtered_df = filtered_df[filtered_df['completion_year'].isin(dropdown_year)]
    if dropdown_component_name:
        filtered_df = filtered_df[filtered_df['component_name'].isin(dropdown_component_name)]
    if dropdown_component_subtype:
        filtered_df = filtered_df[filtered_df['component_subtype'].isin(dropdown_component_subtype)]
    if dropdown_work_type:
        filtered_df = filtered_df[filtered_df['component_work_types'].isin(dropdown_work_type)]
    if dropdown_fatal_crash:
        filtered_df = filtered_df[filtered_df['component_had_fatal_crash'].isin(dropdown_fatal_crash)]

    # Optionally, limit data for initial visualization to prevent overload
    filtered_df = filtered_df.head(500)  # Adjust the number based on performance observations

    # Create GeoJSON data
    geojson_data = create_geojson(filtered_df)

    return geojson_data

if __name__ == '__main__':
    app.run_server(debug=True, port=8010)