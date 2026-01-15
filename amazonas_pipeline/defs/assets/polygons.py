import geopandas as gpd
import pandas as pd

import dagster as dg
from amazonas_pipeline.defs.partitions import year_and_threshold_partitions
import numpy as np


def reduce_to_set_and_join(group: pd.Series) -> str | float:
    group = group.dropna()
    if len(group) == 0:
        return np.nan
    return "+".join(sorted(list(set(group.to_list()))))


def append_country_name(division_name: str | None, country_name: str) -> str | float:
    if pd.isna(division_name):
        return np.nan
    return division_name + " (" + country_name + ")"


@dg.asset(
    key=["polygons", "filtered_to_boundaries"],
    ins={
        "polygons": dg.AssetIn(["ghsl", "polygons"]),
        "boundary": dg.AssetIn(["regions", "boundary"]),
    },
    partitions_def=year_and_threshold_partitions,
    io_manager_key="geodataframe_manager",
    group_name="polygons",
)
def polygons_filtered_to_boundaries(
    polygons: gpd.GeoDataFrame,
    boundary: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    boundary_geo = boundary["geometry"].item()
    return polygons[polygons.intersects(boundary_geo)].reset_index(drop=True)


@dg.multi_asset(
    ins={
        "polygons_filtered": dg.AssetIn(key=["polygons", "filtered_to_boundaries"]),
        "regions": dg.AssetIn(["regions", "lowest_level"]),
    },
    outs={
        "polygons_one_country": dg.AssetOut(key=["polygons", "one_country"], is_required=False, io_manager_key="geodataframe_manager"),
        "polygons_two_countries": dg.AssetOut(key=["polygons", "two_countries"], is_required=False, io_manager_key="geodataframe_manager"),
    },
    partitions_def=year_and_threshold_partitions,
    group_name="polygons",
)
def polygons_one_and_two_countries(
    context: dg.AssetExecutionContext,
    polygons_filtered: gpd.GeoDataFrame,
    regions: gpd.GeoDataFrame,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    joined = polygons_filtered.sjoin(regions, how="inner", predicate="intersects")
    idx_in_two_countries = joined.groupby(level=0)["GID_0"].apply(lambda x: len(set(x)))
    idx_in_two_countries = idx_in_two_countries[idx_in_two_countries > 1]

    if "polygons_one_country" in context.selected_output_names:
        yield dg.Output(joined.drop(index=idx_in_two_countries.index), name="polygons_one_country")

    if "polygons_two_countries" in context.selected_output_names:
        yield dg.Output(joined.loc[idx_in_two_countries.index], name="polygons_two_countries")

@dg.asset(
    key=["polygons", "joined_with_gadm_one"],
    ins={
        "polygons_one_country": dg.AssetIn(key=["polygons", "one_country"]),
    }
)
def polygons_joined_with_gadm_one(polygons_one_country: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    agg_map = {
        "GID_0": reduce_to_set_and_join,
        "GID_1": reduce_to_set_and_join,
        "GID_2": reduce_to_set_and_join,
        "GID_3": reduce_to_set_and_join,
        "GID_4": reduce_to_set_and_join,
        "NAME_0": reduce_to_set_and_join,
        "NAME_1": reduce_to_set_and_join,
        "NAME_2": reduce_to_set_and_join,
        "NAME_3": reduce_to_set_and_join,
        "NAME_4": reduce_to_set_and_join,
        "geometry": "first",
    }

    cities_in_one_country_processed = polygons_one_country.groupby(level=0).agg(
        agg_map,
    )
    cities_in_two_countries_processed = (
        (
            cities_in_two_countries.assign(
                NAME_1=lambda df: df.apply(
                    lambda row: append_country_name(row["NAME_1"], row["GID_0"]),
                    axis=1,
                ),
                NAME_2=lambda df: df.apply(
                    lambda row: append_country_name(row["NAME_2"], row["GID_0"]),
                    axis=1,
                ),
                NAME_3=lambda df: df.apply(
                    lambda row: append_country_name(row["NAME_3"], row["GID_0"]),
                    axis=1,
                ),
                NAME_4=lambda df: df.apply(
                    lambda row: append_country_name(row["NAME_4"], row["GID_0"]),
                    axis=1,
                ),
            )
        )
        .groupby(level=0)
        .agg(agg_map)
    )

    return gpd.GeoDataFrame(
        pd.concat(
            [cities_in_one_country_processed, cities_in_two_countries_processed],
        ).sort_index(),
        crs="ESRI:54009",
        geometry="geometry",
    )
