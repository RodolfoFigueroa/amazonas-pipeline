from pathlib import Path
from typing import Any

import dagster as dg
import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio as rio
from affine import Affine
from rasterio.crs import CRS

from amazonas_pipeline.defs.resources import PathResource


class BaseManager(dg.ConfigurableIOManager):
    path_resource: dg.ResourceDependency[PathResource]
    extension: str

    def _get_path(
        self,
        context: dg.InputContext | dg.OutputContext,
    ) -> Path | dict[str, Path]:
        out_path = Path(self.path_resource.data_path) / "generated"
        fpath = out_path / "/".join(context.asset_key.path)

        if context.has_asset_partitions:
            if len(context.asset_partition_keys) == 1:
                segments = context.asset_partition_key.split("|")
                if len(segments) == 1:
                    fpath = fpath / segments[0]
                elif len(segments) == 2:
                    fpath = fpath / segments[0] / segments[1]
                else:
                    err = "Invalid partition key format."
                    raise ValueError(err)

                final_path = fpath.with_suffix(fpath.suffix + self.extension)
            else:
                final_path = {}
                for key in context.asset_partition_keys:
                    density, total = key.split("|")
                    temp_path = fpath / f"{density}_{total}"
                    temp_path = temp_path.with_suffix(fpath.suffix + self.extension)
                    final_path[key] = temp_path
        else:
            final_path = fpath.with_suffix(fpath.suffix + self.extension)

        return final_path


class PathIOManager(BaseManager):
    def handle_output(self, context: dg.OutputContext, obj: Any) -> None:
        raise NotImplementedError

    def load_input(self, context: dg.InputContext) -> Path | dict[str, Path]:
        path = self._get_path(context)
        if isinstance(path, Path):
            if not path.exists():
                err = f"Path {path} does not exist."
                raise FileNotFoundError(err)
        elif isinstance(path, dict):
            for val in path.values():
                if not val.exists():
                    err = f"Path {val} does not exist."
                    raise FileNotFoundError(err)
        return path


class DataFrameIOManager(BaseManager):
    def handle_output(self, context: dg.OutputContext, obj: pd.DataFrame) -> None:
        out_path = self._get_path(context)

        if isinstance(out_path, dict):
            raise NotImplementedError

        out_path.parent.mkdir(exist_ok=True, parents=True)

        if self.extension in (".csv"):
            obj.to_csv(out_path, index=False)
        elif self.extension in (".xlsx"):
            obj.to_excel(out_path, index=False)
        else:
            err = "Invalid file extension."
            raise ValueError(err)

    def load_input(
        self,
        context: dg.InputContext,
    ) -> pd.DataFrame | dict[str, pd.DataFrame | None]:
        path = self._get_path(context)
        if isinstance(path, Path):
            if self.extension in (".csv"):
                return pd.read_csv(path)
            if self.extension in (".xlsx"):
                return pd.read_excel(path)
            err = "Invalid file extension."
            raise ValueError(err)

        if isinstance(path, dict):
            out = {}
            for key, fpath in path.items():
                if fpath.exists():
                    if self.extension in (".gpkg", ".geojson"):
                        out[key] = gpd.read_file(fpath)
                    elif self.extension in (".csv"):
                        out[key] = pd.read_csv(fpath)
                    elif self.extension in (".xlsx"):
                        out[key] = pd.read_excel(fpath)
                else:
                    out[key] = None
            return out

        err = "Invalid path type."
        raise ValueError(err)


class GeoDataFrameIOManager(BaseManager):
    def handle_output(self, context: dg.OutputContext, obj: gpd.GeoDataFrame) -> None:
        out_path = self._get_path(context)

        if isinstance(out_path, dict):
            raise NotImplementedError

        out_path.parent.mkdir(exist_ok=True, parents=True)
        obj.to_file(out_path, mode="w")

    def load_input(
        self,
        context: dg.InputContext,
    ) -> gpd.GeoDataFrame | dict[str, gpd.GeoDataFrame | None]:
        path = self._get_path(context)
        if isinstance(path, Path):
            return gpd.read_file(path)

        if isinstance(path, dict):
            out = {}
            for key, fpath in path.items():
                if fpath.exists():
                    out[key] = gpd.read_file(fpath)
                else:
                    out[key] = None
            return out

        err = "Invalid path type."
        raise ValueError(err)


class RasterIOManager(BaseManager):
    def _get_raster_and_transform(self, fpath: Path) -> tuple[np.ndarray, Affine, CRS]:
        with rio.open(fpath, "r") as ds:
            data = ds.read(1)
            transform = ds.transform
            crs = ds.crs
        return data, transform, crs

    def handle_output(
        self,
        context: dg.OutputContext,
        obj: tuple[np.ndarray, Affine, CRS],
    ) -> None:
        fpath = self._get_path(context)

        if isinstance(fpath, dict):
            raise NotImplementedError

        fpath.parent.mkdir(exist_ok=True, parents=True)

        arr, transform, crs = obj
        with rio.open(
            fpath,
            "w",
            driver="GTiff",
            count=1,
            height=arr.shape[0],
            width=arr.shape[1],
            dtype="uint16",
            compress="w",
            crs=crs,
            transform=transform,
        ) as ds:
            ds.write(arr, 1)

    def load_input(
        self,
        context: dg.InputContext,
    ) -> tuple[np.ndarray, Affine, CRS] | dict[str, tuple[np.ndarray, Affine, CRS]]:
        path = self._get_path(context)
        if isinstance(path, Path):
            return self._get_raster_and_transform(path)
        if isinstance(path, dict):
            out_dict = {}
            for key, fpath in path.items():
                out_dict[key] = self._get_raster_and_transform(fpath)
            return out_dict
        err = "Invalid path type."
        raise ValueError(err)
