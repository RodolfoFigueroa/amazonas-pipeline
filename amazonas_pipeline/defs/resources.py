import dagster as dg


class PathResource(dg.ConfigurableResource):
    data_path: str
    ghsl_path: str
    entregable_path: str
