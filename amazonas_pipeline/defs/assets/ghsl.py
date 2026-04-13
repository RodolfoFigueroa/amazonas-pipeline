import os
import sys
import tempfile
import warnings
from pathlib import Path

import geopandas as gpd
import numpy as np
import rasterio as rio
import rasterio.features as rio_features
import rasterio.mask as rio_mask
import rasterio.transform as rio_transform
import scipy
import shapely
from affine import Affine
from rasterio.crs import CRS  # ty:ignore[unresolved-import]
from rpy2.robjects import r
from rpy2.robjects.packages import importr
from rpy2.robjects.vectors import FloatVector, ListVector, StrVector

import dagster as dg
from amazonas_pipeline.defs.partitions import (
    year_and_threshold_partitions,
    year_partitions,
)
from amazonas_pipeline.defs.resources import PathResource

if sys.platform == "win32" and sys.version_info >= (3, 8):
    dll_dir = Path(os.environ["R_HOME"]) / "bin" / "x64"
    if not dll_dir.exists() or not dll_dir.is_dir():
        err = f"Expected R DLL directory not found: {dll_dir}"
        raise FileNotFoundError(err)

    os.add_dll_directory(str(dll_dir))


def get_buffered_bounds(df: gpd.GeoDataFrame) -> list[float]:
    return (
        df.to_crs("ESRI:54009")
        .assign(geometry=lambda df: df["geometry"].buffer(150_000))
        .total_bounds.tolist()
    )


@dg.asset(
    key=["ghsl", "base"],
    partitions_def=year_partitions,
    group_name="ghsl",
)
def ghsl_rasters(
    context: dg.AssetExecutionContext,
    path_resource: PathResource,
) -> None:
    flexurba = importr("flexurba")
    r("options(timeout=500)")

    global_dir = (
        Path(path_resource.data_path)
        / "generated"
        / "GHSL"
        / "global"
        / context.partition_key
    )
    global_dir.mkdir(exist_ok=True, parents=True)

    all_exist = True
    for name in ("BUILT_S", "LAND", "POP"):
        for extension in ("tif", "json"):
            fname = global_dir / f"{name}.{extension}"
            all_exist &= fname.exists()

    if not all_exist:
        flexurba.download_GHSLdata(
            output_directory=str(global_dir),
            filenames=StrVector(["BUILT_S.tif", "POP.tif", "LAND.tif"]),
            products=StrVector(["BUILT_S", "POP", "LAND"]),
            epoch=int(context.partition_key),
            resolution=1000,
            crs=54009,
        )


@dg.asset(
    key=["ghsl", "cropped"],
    ins={
        "boundary": dg.AssetIn(key=["regions", "boundary"]),
    },
    partitions_def=year_partitions,
    deps=[dg.AssetDep(["ghsl", "base"])],
    group_name="ghsl",
)
def ghsl_rasters_cropped(
    context: dg.AssetExecutionContext,
    path_resource: PathResource,
    boundary: gpd.GeoDataFrame,
) -> None:
    flexurba = importr("flexurba")
    terra = importr("terra")

    out_path = Path(path_resource.data_path) / "generated"

    xmin, ymin, xmax, ymax = get_buffered_bounds(boundary)

    bbox = FloatVector([xmin, xmax, ymin, ymax])
    bbox.names = StrVector(["xmin", "xmax", "ymin", "ymax"])

    global_dir = out_path / "GHSL" / "global" / context.partition_key

    out_dir = out_path / "GHSL" / "cropped" / context.partition_key
    out_dir.mkdir(exist_ok=True, parents=True)

    for name in ("BUILT_S", "LAND", "POP"):
        for extension in ("tif", "json"):
            fname = out_dir / f"{name}.{extension}"
            if fname.exists():
                fname.unlink()

    flexurba.crop_GHSLdata(
        extent=terra.ext(bbox),
        global_directory=str(global_dir),
        global_filenames=StrVector(["BUILT_S.tif", "POP.tif", "LAND.tif"]),
        output_directory=str(out_dir),
        output_filenames=StrVector(["BUILT_S.tif", "POP.tif", "LAND.tif"]),
    )


@dg.asset(
    key=["ghsl", "smod"],
    deps=[dg.AssetDep(["ghsl", "cropped"])],
    partitions_def=year_and_threshold_partitions,
    io_manager_key="raster_manager",
    group_name="ghsl",
)
def smod_rasters(
    context: dg.AssetExecutionContext,
    path_resource: PathResource,
) -> tuple[np.ndarray, Affine, CRS]:
    flexurba = importr("flexurba")
    terra = importr("terra")

    out_path = Path(path_resource.data_path) / "generated"

    thresholds = context.partition_key.keys_by_dimension["thresholds"].split("_")  # pyright: ignore [reportAttributeAccessIssue]
    year = context.partition_key.keys_by_dimension["year"]  # pyright: ignore [reportAttributeAccessIssue]

    saved_path = out_path / "GHSL" / "cropped" / year

    data_amazonas = flexurba.preprocess_grid(str(saved_path))

    classif = flexurba.classify_grid(
        data_amazonas,
        level1=False,
        parameters=ListVector(
            {
                "RC_density_threshold": int(thresholds[0]),
                "RC_size_threshold": int(thresholds[1]),
            },
        ),
    )

    with tempfile.TemporaryDirectory() as f_dir:
        fpath = Path(f_dir) / "smod.tif"
        terra.writeRaster(classif, str(fpath), overwrite=True)

        with rio.open(fpath, "r") as ds:
            data = ds.read(1)
            transform = ds.transform
            crs = ds.crs

            return data, transform, crs


@dg.op
def process_raster(data: tuple[np.ndarray, Affine, CRS]) -> tuple[np.ndarray, Affine]:
    arr, transform, _ = data
    arr = arr.astype(float)
    arr[arr == -200] = np.nan
    return arr, transform


@dg.op
def crop_pop(
    context: dg.OpExecutionContext,
    path_resource: PathResource,
    data_and_transform: tuple[np.ndarray, Affine],
) -> np.ndarray:
    year = context.multi_partition_key.keys_by_dimension["year"]

    data, transform = data_and_transform
    height, width = data.shape
    bounds = rio_transform.array_bounds(height, width, transform)
    bbox = shapely.geometry.box(*bounds)

    fpath = Path(path_resource.ghsl_path) / "POP_1000" / f"{year}.tif"
    with rio.open(fpath) as ds:
        masked, _ = rio_mask.mask(ds, [bbox], crop=True, nodata=0)
        return masked.squeeze()


@dg.op(out=dg.Out(io_manager_key="geodataframe_manager"))
def threshold_and_polygonize(
    pop: np.ndarray,
    smod_and_transform: tuple[np.ndarray, Affine],
) -> gpd.GeoDataFrame:
    pop_thresh = 0
    smod_thresh = 13

    smod, transform = smod_and_transform

    if smod_thresh <= 10:
        warnings.warn(
            (
                "El threshold de SMOD es menor o igual a 10. Esto causará que "
                "píxeles correspondientes a agua se incluyan en el análisis."
            ),
            stacklevel=2,
        )

    masks = [
        pop >= pop_thresh,
        ~np.isnan(pop),
        smod >= smod_thresh,
    ]
    mask = np.ones(smod.shape, dtype=bool)
    for m in masks:
        mask &= m

    smod_filtered = np.where(mask, smod, 0)
    smod_filtered = smod_filtered > 0
    smod_components, _ = scipy.ndimage.label(
        smod_filtered,
        structure=scipy.ndimage.generate_binary_structure(2, 2),
    )
    feature_geometries = rio_features.shapes(
        smod_components,
        connectivity=8,
        transform=transform,
    )

    pop_temp = pop.copy()
    pop_temp[np.isnan(pop_temp)] = 0
    pop_temp = pop_temp.reshape(-1)

    counts = np.bincount(smod_components.reshape(-1), weights=pop_temp)

    df_out = []
    for geom, value in feature_geometries:
        if value != 0:
            df_out.append(
                {"pop": counts[int(value)], "geometry": shapely.geometry.shape(geom)},
            )
    return (
        gpd.GeoDataFrame(df_out, crs="ESRI:54009")
        .reset_index(names="polygon_id")
        .assign(polygon_id=lambda df: "p" + df["polygon_id"].astype(str).str.zfill(6))
    )


@dg.graph_asset(
    key=["ghsl", "polygons"],
    ins={
        "smod_data": dg.AssetIn(["ghsl", "smod"]),
    },
    partitions_def=year_and_threshold_partitions,
    group_name="ghsl",
)
def polygons_ghsl(
    smod_data: tuple[np.ndarray, Affine, CRS],
) -> gpd.GeoDataFrame:
    smod_and_transform = process_raster(smod_data)
    pop = crop_pop(smod_and_transform)
    return threshold_and_polygonize(pop, smod_and_transform)
