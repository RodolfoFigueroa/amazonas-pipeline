from pathlib import Path

import fiona
import geopandas as gpd
import pandas as pd

import dagster as dg
from amazonas_pipeline.defs.assets.constants import ISO3_TO_NAME
from amazonas_pipeline.defs.resources import PathResource


@dg.asset(
    key=["regions", "country_level"],
    io_manager_key="geodataframe_manager",
    group_name="regions",
)
def regions_country_level(path_resource: PathResource) -> gpd.GeoDataFrame:
    gadm_path = Path(path_resource.data_path) / "initial" / "GADM"

    out = [
        gpd.read_file(path, layer=0)[["geometry"]].assign(
            country=path.stem.replace("gadm41_", ""),
        )
        for path in gadm_path.glob("*.gpkg")
    ]
    return gpd.GeoDataFrame(pd.concat(out, ignore_index=True)).to_crs("ESRI:54009")


@dg.asset(
    key=["regions", "lowest_level"],
    io_manager_key="geodataframe_manager",
    group_name="regions",
)
def regions_lowest_level(path_resource: PathResource) -> gpd.GeoDataFrame:
    gadm_path = Path(path_resource.data_path) / "initial" / "GADM"

    out = []
    for fpath in gadm_path.glob("*.gpkg"):
        layers = fiona.listlayers(fpath)
        df = (
            gpd.read_file(fpath, layer=layers[-1])
            .filter(regex=r"^GID|^NAME|geometry")
            .assign(NAME_0=lambda df: df["GID_0"].map(ISO3_TO_NAME))
        )
        out.append(df)

    return (
        gpd.GeoDataFrame(pd.concat(out, ignore_index=True))
        .to_crs("ESRI:54009")
        .reset_index(names="region_id")
        .assign(region_id=lambda df: "r" + df["region_id"].astype(str).str.zfill(5))
    )


@dg.asset(
    key=["regions", "boundary"],
    ins={
        "regions": dg.AssetIn(["regions", "country_level"]),
    },
    io_manager_key="geodataframe_manager",
    group_name="regions",
)
def boundary(
    regions: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    union = (
        regions.to_crs("ESRI:54009")
        .explode()
        .assign(geometry=lambda df: df["geometry"].buffer(1000).simplify(100))[
            "geometry"
        ]
        .union_all()
    )
    union_large = (
        gpd.GeoDataFrame(geometry=[union], crs="ESRI:54009")
        .explode()
        .assign(area=lambda df: df["geometry"].area)
        .query("area > 1e8")
        .union_all()
    )
    return gpd.GeoDataFrame(geometry=[union_large], crs="ESRI:54009")
