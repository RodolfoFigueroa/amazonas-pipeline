from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio as rio
import rasterio.mask as rio_mask

import dagster as dg
from amazonas_pipeline.defs.resources import PathResource


@dg.op
def add_total_pop(
    path_resource: PathResource,
    polygons: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    ghsl_path = Path(path_resource.ghsl_path)

    out = []
    for year in range(1975, 2021, 5):
        raster_path = ghsl_path / "POP_1000" / f"{year}.tif"
        with rio.open(raster_path) as ds:
            for idx, geom in polygons["geometry"].items():
                masked, _ = rio_mask.mask(ds, [geom], crop=True, nodata=0)
                out.append(
                    {
                        "idx": idx,
                        "year": year,
                        "pop": masked.sum(),
                    },
                )

    pops = (
        pd.DataFrame(out)
        .pivot_table(index="idx", columns="year", values="pop")
        .add_prefix("pop_", axis=1)
    )
    return pd.concat([polygons, pops], axis=1).pipe(
        gpd.GeoDataFrame,
        geometry="geometry",
        crs=polygons.crs,
    )


@dg.op
def add_smod_pop(
    path_resource: PathResource,
    polygons: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    ghsl_path = Path(path_resource.ghsl_path)

    res = []
    for year in range(1975, 2021, 5):
        with (
            rio.open(ghsl_path / "POP_1000" / f"{year}.tif") as ds_pop,
            rio.open(ghsl_path / "SMOD_1000" / f"{year}.tif") as ds_smod,
        ):
            for idx, geom in polygons["geometry"].items():
                masked_pop, _ = rio_mask.mask(ds_pop, [geom], crop=True, nodata=0)
                masked_smod, _ = rio_mask.mask(ds_smod, [geom], crop=True, nodata=0)

                masked_pop = masked_pop.squeeze()
                masked_smod = (masked_smod // 10 * 10).squeeze()

                weighted_count = np.bincount(
                    masked_smod.reshape(-1),
                    weights=masked_pop.reshape(-1),
                )

                res.append(
                    {
                        "idx": idx,
                        "year": year,
                        "pop_urban_center": weighted_count[30]
                        if len(weighted_count) > 30
                        else 0,
                        "pop_urban_cluster": weighted_count[20]
                        if len(weighted_count) > 20
                        else 0,
                        "pop_rural": weighted_count[10]
                        if len(weighted_count) > 10
                        else 0,
                    },
                )

    concat = pd.DataFrame(res)
    df_urban_center = concat.pivot_table(
        index="idx",
        columns="year",
        values="pop_urban_center",
    ).add_prefix("pop_urban_center_")
    df_urban_cluster = concat.pivot_table(
        index="idx",
        columns="year",
        values="pop_urban_cluster",
    ).add_prefix("pop_urban_cluster_")
    df_rural = concat.pivot_table(
        index="idx",
        columns="year",
        values="pop_rural",
    ).add_prefix(
        "pop_rural_",
    )

    return pd.concat(
        [polygons, df_urban_center, df_urban_cluster, df_rural],
        axis=1,
    ).pipe(gpd.GeoDataFrame, geometry="geometry", crs=polygons.crs)


@dg.op(out=dg.Out(io_manager_key="geodataframe_manager"))
def add_area_and_densities(polygons: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    out = polygons.assign(
        area_km2=polygons.to_crs("ESRI:54009")["geometry"].area / 1e6,
    )
    for year in range(1975, 2021, 5):
        out = out.assign(
            **{f"density_{year}": lambda df: df[f"pop_{year}"] / df["area_km2"]},
        )

    return out
