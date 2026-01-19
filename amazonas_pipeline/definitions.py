from pathlib import Path

import dagster as dg
from amazonas_pipeline.defs.managers import (
    DataFrameIOManager,
    GeoDataFrameIOManager,
    RasterIOManager,
)
from amazonas_pipeline.defs.resources import PathResource

# Resources
path_resource = PathResource(
    data_path=dg.EnvVar("DATA_PATH"),
    ghsl_path=dg.EnvVar("GHSL_PATH"),
)
dataframe_manager = DataFrameIOManager(
    path_resource=path_resource,
    extension=".xlsx",
)
geodataframe_manager = GeoDataFrameIOManager(
    path_resource=path_resource,
    extension=".gpkg",
)


@dg.definitions
def defs() -> dg.Definitions:
    return dg.Definitions.merge(
        dg.load_from_defs_folder(path_within_project=Path(__file__).parent),
        dg.Definitions(
            resources={
                "path_resource": path_resource,
                "dataframe_manager": dataframe_manager,
                "geodataframe_manager": geodataframe_manager,
                "raster_manager": RasterIOManager(
                    path_resource=path_resource,
                    extension=".tif",
                ),
            },
        ),
    )
