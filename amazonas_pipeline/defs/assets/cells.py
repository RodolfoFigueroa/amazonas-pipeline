from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio as rio
from amazonas_pipeline.utils import generate_boxes
import shapely
from shapely.prepared import prep

import dagster as dg
from amazonas_pipeline.defs.partitions import year_and_threshold_partitions
from amazonas_pipeline.defs.resources import PathResource


@dg.asset(
    key=["cells", "base"],
    ins={"boundary": dg.AssetIn(["regions", "boundary"])},
    io_manager_key="geodataframe_manager",
    group_name="cells",
)
def cells(
    boundary: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    xmin, ymin, xmax, ymax = boundary.total_bounds

    crs = boundary.crs
    if crs is None:
        err = "Boundary GeoDataFrame must have a CRS defined."
        raise ValueError(err)
    df_boxes = generate_boxes(xmin, ymin, xmax, ymax, crs=crs)

    prepared = prep(boundary["geometry"].item())
    return gpd.GeoDataFrame(
        df_boxes.loc[prepared.intersects(df_boxes["geometry"].to_numpy())],  # pyright: ignore[reportArgumentType]
    )


@dg.asset(
    key=["cells", "countries"],
    ins={
        "df_cells": dg.AssetIn(["cells", "base"]),
        "df_regions": dg.AssetIn(["regions", "country_level"]),
    },
    io_manager_key="geodataframe_manager",
    group_name="cells",
)
def cells_countries(
    df_cells: gpd.GeoDataFrame,
    df_regions: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    df_cells = df_cells.assign(
        cell_id=lambda df: "c" + df["id"].astype(str).str.zfill(8),
    ).drop(columns=["id"])
    df_regions = df_regions.assign(geometry=lambda df: df["geometry"].simplify(1000))

    intersection_map = {}
    for _, row in df_regions.iterrows():
        geom = row["geometry"]
        prepared = prep(geom)
        intersection: pd.Series = df_cells.loc[
            prepared.intersects(df_cells["geometry"].to_numpy()),  # pyright: ignore[reportArgumentType, reportAssignmentType]
            "cell_id",
        ]
        intersection_map[row["country"]] = intersection.tolist()

    cell_intersections = {}
    for country, cell_ids in intersection_map.items():
        for cell_id in cell_ids:
            cell_intersections.setdefault(cell_id, []).append(country)

    df_intersections = (
        pd.Series(cell_intersections, name="countries")
        .to_frame()
        .explode("countries")
        .reset_index(names="cell_id")
        .pivot_table(index="cell_id", columns="countries", aggfunc=len, fill_value=0)
    )

    single_cell_map = (
        df_intersections.loc[df_intersections[df_intersections.sum(axis=1) == 1].index]
        .idxmax(axis=1)
        .to_dict()
    )
    multiple_cell_map = (
        df_cells.set_index("cell_id")
        .loc[df_intersections[df_intersections.sum(axis=1) > 1].index]
        .reset_index()
        .overlay(df_regions, how="intersection")
        .assign(area=lambda df: df["geometry"].area)
        .sort_values("area", ascending=False)
        .drop_duplicates(subset=["cell_id"])
        .set_index("cell_id")["country"]
        .to_dict()
    )
    cells_map = {**single_cell_map, **multiple_cell_map}
    return df_cells.assign(country=lambda df: df["cell_id"].map(cells_map)).dropna(
        subset=["country"],
    )


@dg.asset(
    key=["cells", "joined_with_polygons"],
    ins={
        "df_cells": dg.AssetIn(["cells", "countries"]),
        "df_polygons": dg.AssetIn(["polygons", "unique_name"]),
    },
    partitions_def=year_and_threshold_partitions,
    io_manager_key="geodataframe_manager",
    group_name="cells",
)
def cells_joined_with_polygons(
    df_cells: gpd.GeoDataFrame,
    df_polygons: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    df_centroids = df_cells.assign(geometry=lambda df: df["geometry"].centroid).filter(
        ["cell_id", "geometry"],
    )
    joined = (
        df_polygons[["polygon_id", "geometry"]]
        .sjoin(df_centroids, how="inner", predicate="contains")
        .drop(columns=["index_right"])
    )

    cell_to_polygon_id = joined.set_index("cell_id")["polygon_id"].to_dict()
    cell_id_list = set(joined["cell_id"].tolist())  # noqa: F841
    return (
        df_cells.query("cell_id in @cell_id_list")
        .reset_index(drop=True)
        .assign(polygon_id=lambda df: df["cell_id"].map(cell_to_polygon_id))
    )


@dg.asset(
    key=["cells", "pop_and_smod"],
    ins={"cells": dg.AssetIn(["cells", "joined_with_polygons"])},
    partitions_def=year_and_threshold_partitions,
    io_manager_key="geodataframe_manager",
    group_name="cells",
)
def pop_and_smod(
    path_resource: PathResource,
    cells: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    pop_path = Path(path_resource.ghsl_path) / "POP_1000"
    smod_path = Path(path_resource.ghsl_path) / "SMOD_1000"

    centroid_coords = cells.centroid.get_coordinates().to_numpy()

    for raster_path, prefix in zip([smod_path, pop_path], ["smod", "pop"], strict=True):
        for year in range(1975, 2021, 5):
            pop_raster_path = raster_path / f"{year}.tif"
            with rio.open(pop_raster_path) as ds:
                cells[f"{prefix}_{year}"] = np.array(
                    list(ds.sample(centroid_coords)),
                ).squeeze()

    for year in range(1975, 2021, 5):
        cells[f"smod_{year}"] = cells[f"smod_{year}"].floordiv(10).mul(10)

    return cells
