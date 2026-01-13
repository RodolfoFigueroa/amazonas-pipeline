from pathlib import Path

import dagster as dg
import geopandas as gpd
import pandas as pd
import shapely

from amazonas_pipeline.defs.resources import PathResource


@dg.asset(name="regions", io_manager_key="geodataframe_manager", group_name="regions")
def regions(path_resource: PathResource) -> gpd.GeoDataFrame:
    gadm_path = Path(path_resource.data_path) / "initial" / "GADM"

    out = [
        gpd.read_file(path, layer=0)[["geometry"]] for path in gadm_path.glob("*.gpkg")
    ]
    return gpd.GeoDataFrame(pd.concat(out, ignore_index=True)).to_crs("ESRI:54009")


@dg.asset(
    name="boundary",
    ins={
        "regions": dg.AssetIn(["regions"]),
    },
    io_manager_key="geodataframe_manager",
    group_name="regions",
)
def boundary(
    regions: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    union = gpd.GeoSeries(
        gpd.GeoSeries(
            [regions["geometry"].to_crs("ESRI:54009").union_all()],
            crs="ESRI:54009",
        )
        .explode()
        .reset_index(drop=True),
    )
    main_poly = shapely.Polygon(
        union.loc[union.area.sort_values(ascending=False).index].iloc[0],
    )  # pyright: ignore[reportArgumentType]

    ring_patches = [shapely.Polygon(ring) for ring in main_poly.interiors]
    ring_patches.append(main_poly)

    main_poly_patched = shapely.union_all(ring_patches)
    main_poly_patched = shapely.simplify(
        main_poly_patched,
        tolerance=100,
        preserve_topology=True,
    )

    return gpd.GeoDataFrame(geometry=[main_poly_patched], crs="ESRI:54009")
