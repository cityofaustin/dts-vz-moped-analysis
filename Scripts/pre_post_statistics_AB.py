import os

import geopandas as gpd
import numpy as np
import pandas as pd
import psycopg2
import pytz
from shapely.geometry import shape
# from sodapy import Socrata
from config import DB_VISION_ZERO, DB_MOPED

# DB_VISION_ZERO = {
#     "dbname": os.getenv("VZ_DB_NAME"),
#     "user": os.getenv("VZ_DB_USER"),
#     "password": os.getenv("VZ_DB_PASSWORD"),
#     "host": os.getenv("VZ_DB_HOST"),
# }

# DB_MOPED = {
#     "dbname": os.getenv("MOPED_DB_NAME"),
#     "user": os.getenv("MOPED_DB_USER"),
#     "password": os.getenv("MOPED_DB_PASSWORD"),
#     "host": os.getenv("MOPED_DB_HOST"),
# }

conn_vz = psycopg2.connect(
    dbname=DB_VISION_ZERO["dbname"],
    user=DB_VISION_ZERO["user"],
    host=DB_VISION_ZERO["host"],
    password=DB_VISION_ZERO["password"],
    port=5432,
)

conn_moped = psycopg2.connect(
    dbname=DB_MOPED["dbname"],
    user=DB_MOPED["user"],
    host=DB_MOPED["host"],
    password=DB_MOPED["password"],
    port=5432,
)

# SO_WEB = os.getenv("SO_WEB")
# SO_KEY = os.getenv("SO_KEY")
# SO_SECRET = os.getenv("SO_SECRET")
# SO_TOKEN = os.getenv("SO_TOKEN")
# DATASET_ID = os.getenv("DATASET_ID")


def get_data(query, cursor):
    """
    Get data from database
    """
    cursor.execute(query)
    data = cursor.fetchall()
    field_names = [i[0] for i in cursor.description]
    df = pd.DataFrame(data, columns=field_names)

    return df


def calculate_duration(df, date_col1, date_col2):
    # Function to calculate duration in years
    duration = (df[date_col2] - df[date_col1]).dt.total_seconds() / (365.25 * 24 * 3600)
    return duration


# def publish_data(df):
#     # sodapy
#     soda = Socrata(SO_WEB, SO_TOKEN, username=SO_KEY, password=SO_SECRET, timeout=60)
#     df["line_geometry"] = df["line_geometry"].astype(str)
#     df["substantial_completion_date"] = df["substantial_completion_date"].astype(str)

#     # Replacing missing values with None instead of the default NaN pandas uses
#     df.replace({pd.NA: None}, inplace=True)

#     # Replacing missing values with None instead of the default NaN pandas uses
#     df.replace({pd.NA: None}, inplace=True)
#     df.replace({np.nan: None}, inplace=True)

#     data = df.to_dict(orient="records")
#     res = soda.replace(DATASET_ID, data)
#     print(res)
#     return res


def main():
    cursor_vz = conn_vz.cursor()
    cursor_moped = conn_moped.cursor()

    # Creating moped dataframe
    QUERY_MOPED = """SELECT project_id, project_component_id, geometry, 
    line_geometry, substantial_completion_date, project_name,
    component_name, component_name_full, component_subtype, project_lead,
    component_work_types, type_name FROM component_arcgis_online_view"""

    # Creating moped dataframe
    df_moped = get_data(QUERY_MOPED, cursor_moped)

    # Data frame info
    df_moped.info()

    # Dropping observations where substantial completion date or line geometry is absent
    df_moped_filter = df_moped.dropna(
        subset=["substantial_completion_date", "line_geometry"]
    )
    df_moped_filter.head()

    df_moped_filter.info()

    # Convert timestamp columns to string
    timestamp_columns = ["substantial_completion_date"]

    for col in timestamp_columns:
        df_moped_filter.loc[:, col] = df_moped_filter[col].astype(str)

    # Apply the geometry transformation
    df_moped_filter.loc[:, "geometry"] = df_moped_filter["geometry"].apply(
        lambda x: shape(x) if x is not None else None
    )
    df_moped_filter.loc[:, "line_geometry"] = df_moped_filter["line_geometry"].apply(
        lambda x: shape(x) if x is not None else None
    )

    # Create GeoDataFrame
    gdf_moped = gpd.GeoDataFrame(df_moped_filter, geometry="geometry")

    # Adding a unique ID column
    gdf_moped["moped_component_id"] = gdf_moped["project_component_id"]

    # Creaing vision zero dataframe
    QUERY_CRASH_DATA = """SELECT crash_id, crash_fatal_fl, crash_date,
    road_constr_zone_fl, latitude, longitude, tot_injry_cnt, 
    death_cnt, est_comp_cost FROM atd_txdot_crashes"""

    df_vz = get_data(QUERY_CRASH_DATA, cursor_vz)

    # Keeping only those observations where x-y coordinates are present
    df_vz_filter = df_vz[df_vz["latitude"].notnull() & df_vz["longitude"].notnull()]

    df_vz_filter.info()

    # Convert timestamp columns to string
    timestamp_columns = ["crash_date"]

    for col in timestamp_columns:
        df_vz_filter.loc[:, col] = df_vz_filter[col].astype(str)

    tz = pytz.timezone("US/Central")
    earliest_crash_date = tz.localize(pd.to_datetime(df_vz_filter["crash_date"]).min())
    most_recent_crash_date = tz.localize(
        pd.to_datetime(df_vz_filter["crash_date"]).max()
    )

    # Creating geodataframe
    gdf_vz = gpd.GeoDataFrame(
        df_vz_filter,
        geometry=gpd.points_from_xy(df_vz_filter.longitude, df_vz_filter.latitude),
        crs="EPSG:4326",
    )

    # Creating buffer for joining
    gdf_moped = gdf_moped.set_geometry("line_geometry")
    gdf_moped.set_crs(epsg=4326, inplace=True)
    gdf_moped_proj = gdf_moped.to_crs(epsg=32614)
    buffer_distance = 20

    gdf_moped_proj = gdf_moped.to_crs(epsg=32614)

    gdf_moped_proj["buffered_geometry"] = gdf_moped_proj.geometry.buffer(
        buffer_distance
    )
    buffered_moped_gdf = gdf_moped_proj.set_geometry("buffered_geometry").to_crs(
        "EPSG:4326"
    )

    # Buffered geometry results in line strings and multi line strings being turned into polygons

    buffered_moped_gdf.head()

    # Spatial join
    crashes_near_projects = gpd.sjoin(gdf_vz, buffered_moped_gdf, how="inner")

    # Creating a unique ID column
    crashes_near_projects["crash_project_component_id"] = (
        crashes_near_projects["crash_id"].astype(str)
        + "-"
        + crashes_near_projects["project_id"].astype(str)
        + "-"
        + crashes_near_projects["project_component_id"].astype(str)
    )

    print(
        "Number of unique crashes in merged dataset:",
        crashes_near_projects["crash_id"].nunique(),
    )
    print(
        "Number of unique moped component IDs in merged dataset:",
        crashes_near_projects["moped_component_id"].nunique(),
    )

    crashes_near_projects.info()

    # Formatting crash date
    crashes_near_projects["crash_date"] = (
        pd.to_datetime(crashes_near_projects["crash_date"], errors="coerce")
        .dt.tz_localize("UTC", nonexistent="NaT", ambiguous="NaT")
        .dt.tz_convert("UTC")
    )

    crashes_near_projects.info()

    # Re-arranging columns
    # unique identifier for each observation
    crashes_near_projects.insert(
        0,
        "crash_project_component_id",
        crashes_near_projects.pop("crash_project_component_id"),
    )

    # moped_component_id
    crashes_near_projects.insert(
        2, "moped_component_id", crashes_near_projects.pop("moped_component_id")
    )

    # crash_date
    crashes_near_projects.insert(
        4, "crash_date", crashes_near_projects.pop("crash_date")
    )

    # project component ID
    crashes_near_projects.insert(
        3, "project_component_id", crashes_near_projects.pop("project_component_id")
    )

    # Substantial completion date
    crashes_near_projects.insert(
        5,
        "substantial_completion_date",
        crashes_near_projects.pop("substantial_completion_date"),
    )

    # Creating a binary version of the fatality column
    crashes_near_projects["crash_fatal_binary"] = crashes_near_projects[
        "crash_fatal_fl"
    ].apply(lambda x: 1 if x == "Y" else 0)
    crashes_near_projects.pop("crash_fatal_fl")

    # Rearranging the crash fatal binary column
    crashes_near_projects.insert(
        4, "crash_fatal_binary", crashes_near_projects.pop("crash_fatal_binary")
    )

    crashes_near_projects.head()

    crashes_near_projects["crash_fatal_binary"].value_counts()

    crashes_near_projects.info()

    # Creating indicator variables for crash occurring pre and post completion of mobility project
    crashes_near_projects.insert(
        7,
        "crash_pre_completion",
        crashes_near_projects["crash_date"]
        < crashes_near_projects["substantial_completion_date"],
    )
    crashes_near_projects.insert(
        8,
        "crash_post_completion",
        crashes_near_projects["crash_date"]
        > crashes_near_projects["substantial_completion_date"],
    )

    crashes_near_projects["substantial_completion_date"] = pd.to_datetime(
        crashes_near_projects["substantial_completion_date"]
    )

    # Creating time difference variables
    crashes_near_projects.insert(
        9,
        "crash_project_date_diff",
        crashes_near_projects["substantial_completion_date"]
        - crashes_near_projects["crash_date"],
    )

    # Converting estimated comp cost to float format
    crashes_near_projects["est_comp_cost"] = crashes_near_projects["est_comp_cost"].map(
        lambda x: float(x)
    )

    pre_completion_stats = (
        crashes_near_projects[crashes_near_projects["crash_pre_completion"] == True]
        .groupby("moped_component_id")
        .agg(
            {
                "crash_id": "count",
                "crash_fatal_binary": "sum",
                "tot_injry_cnt": "sum",
                "death_cnt": "sum",
                "est_comp_cost": "sum",
            }
        )
        .rename(
            columns={
                "crash_id": "pre_crash_count",
                "crash_fatal_binary": "pre_fatal_crash_count",
                "tot_injry_cnt": "pre_total_injury_count",
                "death_cnt": "pre_total_death_count",
                "est_comp_cost": "pre_est_comp_cost",
            }
        )
        .reset_index()
    )

    post_completion_stats = (
        crashes_near_projects[crashes_near_projects["crash_post_completion"] == True]
        .groupby("moped_component_id")
        .agg(
            {
                "crash_id": "count",
                "crash_fatal_binary": "sum",
                "tot_injry_cnt": "sum",
                "death_cnt": "sum",
                "est_comp_cost": "sum",
            }
        )
        .rename(
            columns={
                "crash_id": "post_crash_count",
                "crash_fatal_binary": "post_fatal_crash_count",
                "tot_injry_cnt": "post_total_injury_count",
                "death_cnt": "post_total_death_count",
                "est_comp_cost": "post_est_comp_cost",
            }
        )
        .reset_index()
    )

    # Merging
    annualized_statistics = pre_completion_stats.merge(
        post_completion_stats, on="moped_component_id", how="outer"
    ).fillna(0)

    # Getting completion date for each moped component id
    completion_dates = (
        crashes_near_projects.groupby("moped_component_id")[
            "substantial_completion_date"
        ]
        .first()
        .reset_index()
    )

    # Merging into the annualized crash rate DataFrame
    annualized_statistics = annualized_statistics.merge(
        completion_dates, on="moped_component_id", how="left"
    )

    annualized_statistics["years_before_completion"] = (
        annualized_statistics["substantial_completion_date"] - earliest_crash_date
    ).dt.days / 365.25
    annualized_statistics["years_after_completion"] = (
        most_recent_crash_date - annualized_statistics["substantial_completion_date"]
    ).dt.days / 365.25
    # Calculating annualized statistics
    # Crash rates
    annualized_statistics["pre_annualized_crash_rate"] = (
        annualized_statistics["pre_crash_count"]
        / annualized_statistics["years_before_completion"]
    )
    annualized_statistics["post_annualized_crash_rate"] = (
        annualized_statistics["post_crash_count"]
        / annualized_statistics["years_after_completion"]
    )

    # Fatality
    annualized_statistics["pre_annualized_fatal_crash_rate"] = (
        annualized_statistics["pre_fatal_crash_count"]
        / annualized_statistics["years_before_completion"]
    )
    annualized_statistics["post_annualized_fatal_crash_rate"] = (
        annualized_statistics["post_fatal_crash_count"]
        / annualized_statistics["years_after_completion"]
    )

    # Injury count
    annualized_statistics["pre_annualized_injury_rate"] = (
        annualized_statistics["pre_total_injury_count"]
        / annualized_statistics["years_before_completion"]
    )
    annualized_statistics["post_annualized_injury_rate"] = (
        annualized_statistics["post_total_injury_count"]
        / annualized_statistics["years_after_completion"]
    )

    # Death count
    annualized_statistics["pre_annualized_death_rate"] = (
        annualized_statistics["pre_total_death_count"]
        / annualized_statistics["years_before_completion"]
    )
    annualized_statistics["post_annualized_death_rate"] = (
        annualized_statistics["post_total_death_count"]
        / annualized_statistics["years_after_completion"]
    )

    # Estimated cost
    annualized_statistics["pre_annualized_cost"] = (
        annualized_statistics["pre_est_comp_cost"]
        / annualized_statistics["years_before_completion"]
    )
    annualized_statistics["post_annualized_cost"] = (
        annualized_statistics["post_est_comp_cost"]
        / annualized_statistics["years_after_completion"]
    )

    annualized_statistics = annualized_statistics[
        [
            "moped_component_id",
            "substantial_completion_date",
            "pre_annualized_crash_rate",
            "post_annualized_crash_rate",
            "pre_annualized_fatal_crash_rate",
            "post_annualized_fatal_crash_rate",
            "pre_annualized_injury_rate",
            "post_annualized_injury_rate",
            "pre_annualized_death_rate",
            "post_annualized_death_rate",
            "pre_annualized_cost",
            "post_annualized_cost",
        ]
    ]

    # Creating difference columns between pre and post
    annualized_statistics.insert(
        4,
        "delta_crash_rate",
        annualized_statistics["post_annualized_crash_rate"]
        - annualized_statistics["pre_annualized_crash_rate"],
    )
    annualized_statistics.insert(
        7,
        "delta_fatal_crash_rate",
        annualized_statistics["post_annualized_fatal_crash_rate"]
        - annualized_statistics["pre_annualized_fatal_crash_rate"],
    )
    annualized_statistics.insert(
        10,
        "delta_injury_rate",
        annualized_statistics["post_annualized_injury_rate"]
        - annualized_statistics["pre_annualized_injury_rate"],
    )
    annualized_statistics.insert(
        13,
        "delta_death_rate",
        annualized_statistics["post_annualized_death_rate"]
        - annualized_statistics["pre_annualized_death_rate"],
    )
    annualized_statistics.insert(
        16,
        "delta_comp_cost",
        annualized_statistics["post_annualized_cost"]
        - annualized_statistics["pre_annualized_cost"],
    )

    # Merging additional information such as component name, type, etc.
    additional_info = crashes_near_projects[
        [
            "moped_component_id",
            "component_name",
            "component_name_full",
            "component_subtype",
            "component_work_types",
            "type_name",
            "line_geometry",
            'project_name',
            'project_id',
            'project_component_id',
            "project_lead"
        ]
    ].drop_duplicates()

    annualized_statistics = annualized_statistics.merge(
        additional_info, on="moped_component_id", how="left"
    )

    # Reordering
    all_columns = annualized_statistics.columns.tolist()

    first_column = all_columns[0]
    last_six = all_columns[-6:]
    new_order = [first_column] + last_six + all_columns[1:-6]
    annualized_statistics = annualized_statistics[new_order]

    # response = publish_data(annualized_statistics)

    # exporting data locally
    annualized_statistics.to_csv('../Output/annualized_statistics.csv', na_rep="NA", index=False)
    
    return None

main()
