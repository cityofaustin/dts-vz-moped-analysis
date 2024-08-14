# %%
# Package imports
import pandas as pd
import geopandas as gpd
import plotly.express as px  
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, dash_table 
import dash_bootstrap_components as dbc

# %%
# Data imports
vz_moped = pd.read_csv('../Output/annualized_statistics.csv')

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

# %%
vz_moped

# %%
vz_moped[['pre_annualized_fatal_crash_rate', 'post_annualized_fatal_crash_rate']]

# %%
vz_moped['component_had_fatal_crash'].value_counts()

# %%
vz_moped.info()

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

app.layout = dbc.Container([
    dbc.Row(
        dbc.Col(html.H1("Component Analysis (Pre/Post)", className="app_title"),
                width=12),
                className="justify-content-center"
    ),
    dbc.Row([
        dbc.Col(
            [
                create_card("Completion year", "dropdown_year", sorted(vz_moped['completion_year'].unique()))
            ]
        ),
        dbc.Col(
            [
                create_card("Component name", "dropdown_component_name", vz_moped['component_name'].unique())
            ]
        ),
        dbc.Col(
            [
                create_card("Component sub-type", "dropdown_component_subtype", vz_moped['component_subtype'].unique())
            ]
        ),
        dbc.Col(
            [
                create_card("Component work-type", "dropdown_work_type", vz_moped['component_work_types'].unique())
            ]
        )
    ])
])

if __name__ == '__main__':
    app.run(debug=True)
