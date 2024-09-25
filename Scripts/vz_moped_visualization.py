# %%
import json
import dash
from dash import dcc, html, Input, Output
import dash_bootstrap_components as dbc
import dash_leaflet as dl
import pandas as pd
import geopandas as gpd
from shapely import wkt
from pyproj import Transformer
from shapely.geometry import LineString, MultiLineString
import dash_ag_grid as dag

# Data imports
# vz_moped = pd.read_csv('../Output/annualized_statistics.csv')

vz_moped = pd.read_csv('https://data.austintexas.gov/resource/a65x-x4y7.csv?$limit=9999999')

# Dropdown menu formatting
columns_to_display = [
    "project_id",
    "project_component_id",
    "project_lead",
    "component_name",
    "component_subtype",
    "component_work_types",
    "type_name",
    "substantial_completion_date",
    "pre_annualized_crash_rate",
    "post_annualized_crash_rate",
    "delta_crash_rate",
    "pre_annualized_fatal_crash_rate",
    "post_annualized_fatal_crash_rate",
    "delta_fatal_crash_rate",
    "pre_annualized_injury_rate",
    "post_annualized_injury_rate",
    "delta_injury_rate",
    "pre_annualized_death_rate",
    "post_annualized_death_rate",
    "delta_death_rate",
    "pre_annualized_cost",
    "post_annualized_cost",
    "delta_comp_cost",
    "component_had_fatal_crash",
]

column_labels = {
    "project_id": "Project ID",
    "project_component_id": "Project Component ID",
    "project_lead": "Project Lead",    
    "moped_component_id": "Component ID",
    "component_name": "Project Name",
    "component_name_full": "Full Name",
    "component_subtype": "Subtype",
    "component_work_types": "Work Types",
    "type_name": "Type",
    "substantial_completion_date": "Completion Date",
    "pre_annualized_crash_rate": "Pre Annualized Crash Rate",
    "post_annualized_crash_rate": "Post Annualized Crash Rate",
    "delta_crash_rate": "Delta Crash Rate",
    "pre_annualized_fatal_crash_rate": "Pre Annualized Fatal Crash Rate",
    "post_annualized_fatal_crash_rate": "Post Annualized Fatal Crash Rate",
    "delta_fatal_crash_rate": "Delta Fatal Crash Rate",
    "pre_annualized_injury_rate": "Pre Annualized Injury Rate",
    "post_annualized_injury_rate": "Post Annualized Injury Rate",
    "delta_injury_rate": "Delta Injury Rate",
    "pre_annualized_death_rate": "Pre Annualized Death Rate",
    "post_annualized_death_rate": "Post Annualized Death Rate",
    "delta_death_rate": "Delta Death Rate",
    "pre_annualized_cost": "Pre Annualized Composite Cost",
    "post_annualized_cost": "Post Annualized Composite Cost",
    "delta_comp_cost": "Delta Composite Cost",
    "component_had_fatal_crash": "Had Fatal Crash",
}

# Data manipulation

# Creating new project ID column
vz_moped["project_id"] = vz_moped['project_id'].astype(str) + " : " + vz_moped['project_name'].astype(str)

# Fixing the type of the project lead column
vz_moped['project_lead'] = vz_moped['project_lead'].astype(str)

# Renaming columns
vz_moped.rename(
    columns={
        'pre_annualized_fatal_crash': 'pre_annualized_fatal_crash_rate',
        'post_annualized_fatal_crash': 'post_annualized_fatal_crash_rate'
    },
    inplace=True
)

# Rounding columns which will be displayed on the table
columns_to_round = [
    "pre_annualized_crash_rate",
    "post_annualized_crash_rate",
    "delta_crash_rate",
    "pre_annualized_fatal_crash_rate",
    "post_annualized_fatal_crash_rate",
    "delta_fatal_crash_rate",
    "pre_annualized_injury_rate",
    "post_annualized_injury_rate",
    "delta_injury_rate",
    "pre_annualized_death_rate",
    "post_annualized_death_rate",
    "delta_death_rate",
    "pre_annualized_cost",
    "post_annualized_cost",
    "delta_comp_cost"
]

DECIMALS = 2    
vz_moped[columns_to_round] = vz_moped[columns_to_round].apply(lambda x: round(x, DECIMALS))

# Changing null values to "N/As"
vz_moped["component_subtype"] = vz_moped["component_subtype"].fillna("N/A")
vz_moped["component_work_types"] = vz_moped["component_work_types"].fillna("N/A")

# Converting to datetime
vz_moped["substantial_completion_date"] = pd.to_datetime(
    vz_moped["substantial_completion_date"]
)

# Creating completion year variable
vz_moped["completion_year"] = vz_moped["substantial_completion_date"].dt.year

# Keeping only the date in the substantial completition date column
vz_moped["substantial_completion_date"] = vz_moped["substantial_completion_date"].dt.date

# Dropping extreme dates
vz_moped = vz_moped[
    (vz_moped["substantial_completion_date"] <= pd.to_datetime("2030-12-31").date()) &
    (vz_moped["substantial_completion_date"] >= pd.to_datetime("1986-01-01").date())
]

# Flag for involving fatality
# 1 if pre/post fatal crash rate is not null
vz_moped["component_had_fatal_crash"] = (
    vz_moped["pre_annualized_fatal_crash_rate"] > 0
) | (vz_moped["post_annualized_fatal_crash_rate"] > 0)
vz_moped["component_had_fatal_crash"] = vz_moped["component_had_fatal_crash"].apply(
    lambda x: "Yes" if x else "No"
)

# Setting the geometry column
vz_moped["line_geometry"] = vz_moped["line_geometry"].apply(wkt.loads)

transformer = Transformer.from_crs("EPSG:32614", "EPSG:4326", always_xy=True)

# Function to transform coordinates using a transformer
def transform_coordinates(geom, transformer):
    if geom.geom_type == "Point":
        x, y = geom.x, geom.y
        return Point(*transformer.transform(x, y)[::-1])
    elif geom.geom_type == "LineString":
        return LineString([transformer.transform(x, y)[::-1] for x, y in geom.coords])
    elif geom.geom_type == "MultiLineString":
        return MultiLineString(
            [
                LineString([transformer.transform(x, y)[::-1] for x, y in line.coords])
                for line in geom.geoms
            ]
        )
    else:
        raise ValueError(f"Unsupported geometry type: {geom.geom_type}")


# Function to create Geojson data
def create_geojson(filtered_df):
    if filtered_df.empty:
        return {}

    gdf = gpd.GeoDataFrame(filtered_df, geometry="line_geometry")

    # Convert datetime columns to string for geojson export
    for column in gdf.select_dtypes(include=["datetimetz", "datetime64"]).columns:
        gdf[column] = gdf[column].astype(str)

    # Set up the transformer
    transformer = Transformer.from_crs("epsg:32614", "epsg:4326", always_xy=True)

    # Transform the coordinates
    gdf["line_geometry"] = gdf["line_geometry"].apply(
        lambda geom: transform_coordinates(geom, transformer)
    )

    # Get GeoJSON data
    geojson_data = (gdf.__geo_interface__)

    # print(f"\n\nGeoJSON data:\n{json.dumps(geojson_data, indent=4)}")

    # Add the necessary properties for tooltip
    for feature in geojson_data['features']:
        properties = feature['properties']
        tooltip_content = f"""
            <b>Component Name:</b> {properties.get('component_name', 'N/A')}<br>
            <b>Completion Year:</b> {properties.get('completion_year', 'N/A')}<br>
            <b>Component Subtype:</b> {properties.get('component_subtype', 'N/A')}<br>
            <b>Work Type:</b> {properties.get('component_work_types', 'N/A')}<br>
            <b>Fatal Crash:</b> {properties.get('component_had_fatal_crash', 'N/A')}<br>
            <b>Delta Composite Cost:</b> {properties.get('delta_comp_cost', 'N/A')}
        """
        feature['properties']['tooltip'] = tooltip_content

    # Flipping the coordinates
    geojson_data = flip_coordinates(geojson_data)

    return geojson_data


def flip_coordinates(geojson):
    # Parse the GeoJSON string into a Python dictionary

    def flip_coords(coords):
        # Flip the lat and long for a given coordinate pair
        return [coords[1], coords[0]]

    def process_geometry(geometry):
        if geometry["type"] == "Point":
            geometry["coordinates"] = flip_coords(geometry["coordinates"])
        elif geometry["type"] == "LineString" or geometry["type"] == "MultiPoint":
            geometry["coordinates"] = [
                flip_coords(coord) for coord in geometry["coordinates"]
            ]
        elif geometry["type"] == "Polygon" or geometry["type"] == "MultiLineString":
            geometry["coordinates"] = [
                [flip_coords(coord) for coord in ring]
                for ring in geometry["coordinates"]
            ]
        elif geometry["type"] == "MultiPolygon":
            geometry["coordinates"] = [
                [[flip_coords(coord) for coord in ring] for ring in polygon]
                for polygon in geometry["coordinates"]
            ]
        elif geometry["type"] == "GeometryCollection":
            for geom in geometry["geometries"]:
                process_geometry(geom)
        return geometry

    def process_feature(feature):
        feature["geometry"] = process_geometry(feature["geometry"])
        return feature

    if geojson["type"] == "FeatureCollection":
        geojson["features"] = [
            process_feature(feature) for feature in geojson["features"]
        ]
    elif geojson["type"] == "Feature":
        geojson["geometry"] = process_geometry(geojson["geometry"])
    else:  # For direct geometries
        geojson = process_geometry(geojson)

    # Convert the modified dictionary back to a JSON string
    return geojson


app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])


def create_card(title, dropdown_id, options):
    return dbc.Card(
        [
            dbc.CardHeader(title, className="card-header"),
            dbc.CardBody(
                dcc.Dropdown(
                    id=dropdown_id,
                    options=[{"label": i, "value": i} for i in options],
                    multi=True
                ), className="card=body"
            ),
        ]
    , class_name="filter-cards")


app.layout = dbc.Container(
    [
        dbc.Row(
            dbc.Col(html.H1("VisionZero Moped Pre/Post Analysis", className="app_title"), width=12),
            className="justify-content-center",
        ),
        dbc.Row(
            [
                dbc.Col(
                    create_card(
                        "Project ID",
                        "dropdown_project_id",
                        sorted(vz_moped["project_id"].unique()),
                    ),
                    className="navigation_row_col_style",
                ),
                dbc.Col(
                    create_card(
                        "Project Lead",
                        "dropdown_project_lead",
                        sorted(vz_moped["project_lead"].unique()),
                    ),
                    className="navigation_row_col_style",
                ),
                dbc.Col(
                    create_card(
                        "Completion year",
                        "dropdown_year",
                        sorted(vz_moped["completion_year"].unique()),
                    ),
                    className="navigation_row_col_style",
                ),
                dbc.Col(
                    create_card(
                        "Component name",
                        "dropdown_component_name",
                        vz_moped["component_name"].unique(),
                    ),
                    className="navigation_row_col_style",
                ),
                dbc.Col(
                    create_card(
                        "Component sub-type",
                        "dropdown_component_subtype",
                        vz_moped["component_subtype"].unique(),
                    ),
                    className="navigation_row_col_style",
                ),
                dbc.Col(
                    create_card(
                        "Component work-type",
                        "dropdown_work_type",
                        vz_moped["component_work_types"].unique(),
                    ),
                    className="navigation_row_col_style",
                ),
                dbc.Col(
                    create_card(
                        "Fatal crash",
                        "dropdown_fatal_crash",
                        vz_moped["component_had_fatal_crash"].unique(),
                    ),
                    className="navigation_row_col_style",
                ),
            ],
            className="navigation_row",
        ),
        dbc.Row(
            [
                dbc.Col(
                    dag.AgGrid(
                        id='data_table',
                        columnDefs=[
                            {"headerName": column_labels[col], "field": col} for col in columns_to_display
                        ],
                        rowData=vz_moped[columns_to_display].to_dict('records'),
                        defaultColDef={"sortable": True, "filter": True, "resizable": True},
                        style={"height": "700px", "width": "100%"},
                        dashGridOptions={'pagination':True},
                        className="ag-theme-alpine",
                    ),
                    width=8,
                ),
                dbc.Col(
                    dl.Map(
                        center=[30.2672, -97.7431],
                        zoom=12,
                        children=[
                            dl.TileLayer(),
                            dl.ScaleControl(
                                position="bottomleft",
                                imperial=True,
                                metric=False
                            ),
                            dl.GestureHandling(),
                            dl.GeoJSON(
                                id="geojson",
                                options=dict(style={"color": "blue", "weight": 2}),
                                hoverStyle=dict(weight=5, color='#666', dashArray=''),
                                children=[
                                    dl.Tooltip(id="geojson_tooltip", direction='top', permanent=True, className='tooltip')
                                ]
                            ),
                        ],
                        style={"width": "100%", "height": "700px"},
                        className="map",
                        attributionControl=False,
                    ),
                    width=4,
                ),
            ],
        ),
    ],
    fluid=True,
    className="custom_container",
)


@app.callback(
    Output('data_table', 'rowData'),
    Output("geojson", "data"),
    [
        Input("dropdown_project_id", "value"),
        Input("dropdown_project_lead", "value"),
        Input("dropdown_year", "value"),
        Input("dropdown_component_name", "value"),
        Input("dropdown_component_subtype", "value"),
        Input("dropdown_work_type", "value"),
        Input("dropdown_fatal_crash", "value"),
    ],
)
def update_plot(
    dropdown_project_id,
    dropdown_project_lead,
    dropdown_year,
    dropdown_component_name,
    dropdown_component_subtype,
    dropdown_work_type,
    dropdown_fatal_crash,
):
    # Base DataFrame
    filtered_df = vz_moped

    # Convert single values to list to avoid type errors
    dropdown_project_id = (
        [dropdown_project_id]
        if dropdown_project_id and not isinstance(dropdown_project_id, list)
        else dropdown_project_id
    )
    dropdown_project_lead = (
        [dropdown_project_lead]
        if dropdown_project_lead and not isinstance(dropdown_project_lead, list)
        else dropdown_project_lead
    )
    dropdown_year = (
        [dropdown_year]
        if dropdown_year and not isinstance(dropdown_year, list)
        else dropdown_year
    )
    dropdown_component_name = (
        [dropdown_component_name]
        if dropdown_component_name and not isinstance(dropdown_component_name, list)
        else dropdown_component_name
    )
    dropdown_component_subtype = (
        [dropdown_component_subtype]
        if dropdown_component_subtype
        and not isinstance(dropdown_component_subtype, list)
        else dropdown_component_subtype
    )
    dropdown_work_type = (
        [dropdown_work_type]
        if dropdown_work_type and not isinstance(dropdown_work_type, list)
        else dropdown_work_type
    )
    dropdown_fatal_crash = (
        [dropdown_fatal_crash]
        if dropdown_fatal_crash and not isinstance(dropdown_fatal_crash, list)
        else dropdown_fatal_crash
    )

    # Apply filters based on dropdown selection
    if dropdown_project_id:
        filtered_df = filtered_df[filtered_df["project_id"].isin(dropdown_project_id)]
    if dropdown_project_lead:
        filtered_df = filtered_df[filtered_df["project_lead"].isin(dropdown_project_lead)]
    if dropdown_year:
        filtered_df = filtered_df[filtered_df["completion_year"].isin(dropdown_year)]
    if dropdown_component_name:
        filtered_df = filtered_df[
            filtered_df["component_name"].isin(dropdown_component_name)
        ]
    if dropdown_component_subtype:
        filtered_df = filtered_df[
            filtered_df["component_subtype"].isin(dropdown_component_subtype)
        ]
    if dropdown_work_type:
        filtered_df = filtered_df[
            filtered_df["component_work_types"].isin(dropdown_work_type)
        ]
    if dropdown_fatal_crash:
        filtered_df = filtered_df[
            filtered_df["component_had_fatal_crash"].isin(dropdown_fatal_crash)
        ]

    # Create Geojson data
    geojson_data = create_geojson(filtered_df)

    return filtered_df[columns_to_display].to_dict('records'), geojson_data


if __name__ == "__main__":
    app.run_server(debug=False, host="0.0.0.0", port=8050)


