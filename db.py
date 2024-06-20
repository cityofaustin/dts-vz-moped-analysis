"""
Script to combine VisionZero and Moped data
"""

import psycopg2
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape

from config import DB_VISION_ZERO, DB_MOPED
from helper import dict_factory, to_json_list

def get_data(query, cursor):
    """
    Get data from database
    """
    cursor.execute(query)
    data = cursor.fetchall()
    field_names = [i[0] for i in cursor.description]
    df = pd.DataFrame(data, columns=field_names)

    return df

conn_vz = psycopg2.connect(
    dbname = DB_VISION_ZERO['dbname'],
    user = DB_VISION_ZERO["user"],
    host = DB_VISION_ZERO["host"],
    password = DB_VISION_ZERO["password"],
    port=5432
)

conn_moped = psycopg2.connect(
    dbname = DB_MOPED["dbname"],
    user = DB_MOPED["user"],
    host = DB_MOPED["host"],
    password = DB_MOPED["password"],
    port = 5432
)

cursor_vz = conn_vz.cursor()
cursor_moped = conn_moped.cursor()

QUERY_CRASH_DATA = """SELECT crash_id, crash_fatal_fl, crash_date, rpt_latitude,
rpt_longitude, road_constr_zone_fl, latitude, longitude, tot_injry_cnt, death_cnt 
FROM atd_txdot_crashes"""

QUERY_MOPED = """SELECT project_id, project_component_id, geometry, line_geometry, 
substantial_completion_date_estimated, completion_date, completion_end_date
FROM component_arcgis_online_view"""

# Creating moped dataframe
df_moped = get_data(QUERY_MOPED, cursor_moped)
df_moped["geometry"] = df_moped["geometry"].apply(lambda x: shape(x) if x is not None else None)
gdf_moped = gpd.GeoDataFrame(df_moped, geometry="geometry")

gdf_moped.explore()

# Creaing vision zero dataframe
df_vz = get_data(QUERY_CRASH_DATA, cursor_vz)
gdf_vz = gpd.GeoDataFrame(df_vz,
                          geometry=gpd.points_from_xy(df_vz.longitude,
                                                      df_vz.latitude),
                                                      crs='EPSG:4326')

gdf_vz.head()

# cursor_vz.execute(QUERY_CRASH_DATA)
# row = cursor_vz.fetchone()

# print(row)
