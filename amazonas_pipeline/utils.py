import shapely
from pyproj import CRS
import pandas as pd
import os
from pathlib import Path

import geopandas as gpd
import numpy as np
import rasterio as rio


def join_cells_with_polygons(
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
    cell_id_list = set(joined["cell_id"].tolist())
    return (
        df_cells.loc[lambda df: df["cell_id"].isin(cell_id_list)]
        .reset_index(drop=True)
        .assign(polygon_id=lambda df: df["cell_id"].map(cell_to_polygon_id))
    )


def add_pop_and_smod_to_cells(
    cells: gpd.GeoDataFrame,
    *,
    selected_year: int,
    ghsl_path: os.PathLike,
) -> gpd.GeoDataFrame:
    ghsl_path = Path(ghsl_path)

    pop_path = ghsl_path / "POP_1000"
    smod_path = ghsl_path / "SMOD_1000"

    centroid_coords = cells.centroid.get_coordinates().to_numpy()

    for raster_path, prefix in zip([smod_path, pop_path], ["smod", "pop"], strict=True):
        for year in range(1975, selected_year + 1, 5):
            pop_raster_path = raster_path / f"{year}.tif"
            with rio.open(pop_raster_path) as ds:
                cells[f"{prefix}_{year}"] = np.array(
                    list(ds.sample(centroid_coords)),
                ).squeeze()

    for year in range(1975, selected_year + 1, 5):
        cells[f"smod_{year}"] = cells[f"smod_{year}"].floordiv(10).mul(10)

    return cells


def generate_boxes(xmin: float, ymin: float, xmax: float, ymax: float, *, crs: CRS | str) -> gpd.GeoDataFrame:
    xmin = int(np.floor(xmin / 1000) * 1000)
    ymin = int(np.floor(ymin / 1000) * 1000)
    xmax = int(np.ceil(xmax / 1000) * 1000)
    ymax = int(np.ceil(ymax / 1000) * 1000)

    xrange = list(range(xmin, xmax + 1000, 1000))
    yrange = list(range(ymin, ymax + 1000, 1000))

    df_boxes = [
        {
            "id": int(i * len(yrange) + j),
            "geometry": shapely.box(x_start, y_start, x_start + 1000, y_start + 1000),
        }
        for i, x_start in enumerate(xrange)
        for j, y_start in enumerate(yrange)
    ]
    return gpd.GeoDataFrame(
        pd.DataFrame(df_boxes).set_index("id"),
        crs=crs,
        geometry="geometry",
    )


def get_area_by_smod_from_polys(
    df: gpd.GeoDataFrame,
    df_cells: gpd.GeoDataFrame,
    year: int,
) -> pd.DataFrame:
    return (
        join_cells_with_polygons(
            df_cells[["cell_id", f"smod_{year}", "geometry"]],
            df[["geometry"]].reset_index(names="polygon_id"),
        )
        .assign(
            **{
                f"smod_{year}": lambda df: df[f"smod_{year}"].map(
                    {
                        10: "area_rural_km2",
                        20: "area_urban_cluster_km2",
                        30: "area_urban_center_km2",
                    },
                ),
            },
        )
        .groupby(["polygon_id", f"smod_{year}"])
        .size()
        .reset_index()
        .pivot_table(index="polygon_id", columns=f"smod_{year}", values=0, fill_value=0)
        .astype(int)
    )