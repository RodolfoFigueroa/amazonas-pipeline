import geopandas as gpd
import numpy as np
import pandas as pd

import dagster as dg
from amazonas_pipeline.defs.assets.constants import ISO3_TO_NAME
from amazonas_pipeline.defs.partitions import year_and_threshold_partitions


def remove_non_country(id_list: str, country: str) -> str | float:
    out = []
    if id_list is None or (isinstance(id_list, float) and np.isnan(id_list)):
        return np.nan

    for elem in id_list.split("+"):
        if country in elem:
            out.append(elem)
    if len(out) == 0:
        return np.nan
    return "+".join(out)


@dg.asset(
    key=["polygons_split", "base"],
    ins={
        "df_polygons": dg.AssetIn(["polygons", "population"]),
        "df_cells": dg.AssetIn(["cells", "pop_and_smod"]),
    },
    partitions_def=year_and_threshold_partitions,
    io_manager_key="geodataframe_manager",
    group_name="polygons_split",
)
def polygons_split(
    df_polygons: gpd.GeoDataFrame,
    df_cells: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    df_polygons = df_polygons.assign(countries=lambda df: df["GID_0"].str.split("+"))

    single_polygons = df_polygons.query("countries.str.len() == 1").drop(
        columns=["countries"],
    )
    multiple_polygons = (
        df_polygons.query("countries.str.len() > 1")
        .explode("countries")
        .assign(duplicate_id=lambda df: df.groupby("polygon_id").cumcount())
        .assign(
            duplicate_polygon_id=lambda df: df["polygon_id"].astype(str)
            + "_"
            + df["duplicate_id"].astype(str),
        )
        .drop(
            columns=["GID_0", "NAME_0", "geometry"]
            + [
                f"pop{infix}_{year}"
                for year in range(1975, 2021, 5)
                for infix in ["", "_rural", "_urban_center", "_urban_cluster"]
            ],
        )
        .rename(columns={"countries": "GID_0"})
    )

    for prefix in ["GID", "NAME"]:
        for i in range(1, 5):
            multiple_polygons = multiple_polygons.assign(
                **{
                    f"{prefix}_{i}": lambda df: df.apply(
                        lambda row: remove_non_country(
                            row[f"{prefix}_{i}"],
                            row["GID_0"],
                        ),
                        axis=1,
                    ),
                },
            )

    for col in ["name", "max_name"]:
        multiple_polygons = multiple_polygons.assign(
            **{
                col: lambda df: df.apply(
                    lambda row: remove_non_country(row[col], row["GID_0"]),
                    axis=1,
                ),
            },
        )

    cells_merged_with_polygons = (
        multiple_polygons.assign(
            NAME_0=lambda df: df["GID_0"].map(ISO3_TO_NAME),
        )
        .merge(
            df_cells,
            on="polygon_id",
            how="inner",
        )
        .query("country == GID_0")
        .pipe(gpd.GeoDataFrame, geometry="geometry", crs=df_cells.crs)
    )

    total_pops = cells_merged_with_polygons.dissolve(
        "duplicate_polygon_id",
        {
            **{f"GID_{i}": "first" for i in range(5)},
            **{f"NAME_{i}": "first" for i in range(5)},
            "name": "first",
            "max_name": "first",
            **{f"pop_{year}": "sum" for year in range(1975, 2021, 5)},
        },
    )

    pops_by_smod: list[pd.DataFrame] = []
    for year in range(1975, 2021, 5):
        temp = (
            cells_merged_with_polygons.groupby(["duplicate_polygon_id", f"smod_{year}"])
            .agg({f"pop_{year}": "sum"})
            .reset_index()
            .pivot_table(
                index="duplicate_polygon_id",
                columns=f"smod_{year}",
                values=f"pop_{year}",
                fill_value=0,
            )
            .rename(columns={10: "rural", 20: "urban_cluster", 30: "urban_center"})
            .add_prefix("pop_")
            .add_suffix(f"_{year}")
        )
        pops_by_smod.append(temp)

    pops_by_smod_df = pd.concat(pops_by_smod, axis=1)
    final_pops = (
        pd.concat([total_pops, pops_by_smod_df], axis=1)
        .reset_index()
        .drop(columns=["polygon_id"], errors="ignore")
        .rename(columns={"duplicate_polygon_id": "polygon_id"})
    )

    return (
        pd.concat(
            [single_polygons, final_pops],
            axis=0,
            ignore_index=True,
        )
        .sort_values("polygon_id")
        .pipe(gpd.GeoDataFrame, geometry="geometry", crs=df_polygons.crs)
    )
