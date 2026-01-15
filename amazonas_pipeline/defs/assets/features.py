from pathlib import Path

import geopandas as gpd
import osmium
import pandas as pd
from osmium.filter import EmptyTagFilter, GeoInterfaceFilter, TagFilter

import dagster as dg
from amazonas_pipeline.defs.resources import PathResource


@dg.asset(
    key=["features", "filtered_places"],
    group_name="features",
    io_manager_key="geodataframe_manager",
)
def filtered_places(path_resource: PathResource) -> gpd.GeoDataFrame:
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
            .filter(["place", "name", "geometry"], axis=1),
        )

    return gpd.GeoDataFrame(
        pd.concat(out, ignore_index=True),
        geometry="geometry",
        crs="EPSG:4326",
    ).to_crs("ESRI:54009")
