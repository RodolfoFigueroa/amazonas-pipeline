import dagster as dg
from amazonas_pipeline.defs.partitions import year_and_threshold_partitions
import geopandas as gpd
import pandas as pd


COLUMN_ORDER = [
    "polygon_id",
    "NAME_0",
    "name",
    "max_name",
    "NAME_1",
    "NAME_2",
    "NAME_3",
    "NAME_4",
    "GID_0",
    "GID_1",
    "GID_2",
    "GID_3",
    "GID_4",
] + [f"pop{infix}_{year}" for year in range(1975, 2021, 5) for infix in ["", "_rural", "_urban_center", "_urban_cluster"]]

def sheets_factory(key: list[str], in_key: list[str]) -> dg.AssetsDefinition:
    @dg.asset(
        key=key,
        ins={"df_polygons": dg.AssetIn(in_key)},
        partitions_def=year_and_threshold_partitions,
        io_manager_key="dataframe_manager",
        group_name="sheets",
    )
    def _asset(df_polygons: gpd.GeoDataFrame) -> pd.DataFrame:
        return df_polygons[[col for col in COLUMN_ORDER if col in df_polygons.columns]].rename(columns={"NAME_0": "COUNTRY"})

    return _asset

sheets_asset = [
    sheets_factory(["sheets", "polygons"], ["polygons", "population"]),
    sheets_factory(["sheets", "polygons_split"], ["polygons_split", "base"]),
]