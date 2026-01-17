from pathlib import Path

import geopandas as gpd
import osmium
import pandas as pd
import rasterio as rio
import rasterio.mask as rio_mask
from osmium.filter import EmptyTagFilter, GeoInterfaceFilter, TagFilter

import dagster as dg
from amazonas_pipeline.defs.resources import PathResource


def process_geofabrik_files(path_resource: PathResource) -> gpd.GeoDataFrame:
    geofabrik_path = Path(path_resource.data_path) / "initial" / "geofabrik"

    out = []
    for path in geofabrik_path.glob("*.pbf"):
        data = (
            osmium.FileProcessor(path)
            .with_filter(EmptyTagFilter())
            .with_filter(
                TagFilter(
                    ("place", "city"),
                    ("place", "town"),
                    ("place", "village"),
                    ("place", "hamlet"),
                ),
            )
            .with_filter(GeoInterfaceFilter())
        )
        out.append(
            gpd.GeoDataFrame.from_features(data)  # pyright: ignore[reportArgumentType]
            .set_crs("EPSG:4326", allow_override=True)
            .filter(["place", "name", "geometry"], axis=1)
            .dropna(subset=["name"]),
        )

    return gpd.GeoDataFrame(
        pd.concat(out, ignore_index=True),
        geometry="geometry",
        crs="EPSG:4326",
    ).to_crs("ESRI:54009")


def add_countries_to_features(
    features: gpd.GeoDataFrame,
    regions: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    joined = features.sjoin(
        regions[
            [
                "GID_0",
                "GID_1",
                "GID_2",
                "GID_3",
                "GID_4",
                "NAME_0",
                "NAME_1",
                "NAME_2",
                "NAME_3",
                "NAME_4",
                "geometry",
            ]
        ],
        how="left",
        predicate="within",
    )
    return joined.drop(columns=["index_right"]).dropna(subset=["GID_0"])


def add_feature_pop(
    path_resource: PathResource,
    features: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    ghsl_path = Path(path_resource.ghsl_path)

    features_buffered = (
        features.filter(["feature_id", "geometry"], axis=1)
        .to_crs("ESRI:54009")
        .assign(geometry=lambda df: df["geometry"].buffer(10_000))
    )

    pops = []
    with rio.open(ghsl_path / "POP_1000" / "2020.tif") as ds:
        for idx, geom in features_buffered["geometry"].items():
            masked, _ = rio_mask.mask(ds, [geom], crop=True, nodata=0)
            pops.append({"idx": idx, "feature_pop": masked.sum()})

    return features.assign(
        feature_pop=pd.DataFrame(pops).set_index("idx")["feature_pop"],
    )


def add_feature_id(features: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    return (
        features.reset_index(drop=True)
        .reset_index(names="feature_id")
        .assign(
            feature_id=lambda df: "f" + df["feature_id"].astype(str).str.zfill(7),
        )
    )


@dg.asset(
    key=["features", "filtered_places"],
    ins={"regions": dg.AssetIn(["regions", "lowest_level"])},
    io_manager_key="geodataframe_manager",
    group_name="features",
)
def filtered_places(
    path_resource: PathResource,
    regions: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    processed = process_geofabrik_files(path_resource)
    with_countries = add_countries_to_features(processed, regions)
    with_pop = add_feature_pop(path_resource, with_countries)
    return add_feature_id(with_pop)
