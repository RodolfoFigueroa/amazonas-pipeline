from ast import literal_eval
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio as rio
import rasterio.mask as rio_mask

import dagster as dg
from amazonas_pipeline.defs.partitions import year_and_threshold_partitions
from amazonas_pipeline.defs.resources import PathResource


def reduce_to_set_and_join(group: pd.Series) -> str | float:
    group = group.dropna()
    if len(group) == 0:
        return np.nan
    return "+".join(sorted(set(group.to_list())))


def append_country_name(division_name: str | None, country_name: str) -> str | float:
    if pd.isna(division_name):
        return np.nan
    return division_name + " (" + country_name + ")"


@dg.asset(
    key=["polygons", "filtered_to_boundaries"],
    ins={
        "polygons": dg.AssetIn(["ghsl", "polygons"]),
        "boundary": dg.AssetIn(["regions", "boundary"]),
    },
    partitions_def=year_and_threshold_partitions,
    io_manager_key="geodataframe_manager",
    group_name="polygons",
)
def polygons_filtered_to_boundaries(
    polygons: gpd.GeoDataFrame,
    boundary: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    boundary_geo = boundary["geometry"].item()
    return polygons[polygons.intersects(boundary_geo)].reset_index(drop=True)


@dg.multi_asset(
    ins={
        "polygons_filtered": dg.AssetIn(key=["polygons", "filtered_to_boundaries"]),
        "regions": dg.AssetIn(["regions", "lowest_level"]),
    },
    outs={
        "polygons_one_country": dg.AssetOut(
            key=["polygons", "one_country"],
            is_required=False,
            io_manager_key="geodataframe_manager",
        ),
        "polygons_two_countries": dg.AssetOut(
            key=["polygons", "two_countries"],
            is_required=False,
            io_manager_key="geodataframe_manager",
        ),
    },
    partitions_def=year_and_threshold_partitions,
    group_name="polygons",
)
def polygons_one_and_two_countries(
    context: dg.AssetExecutionContext,
    polygons_filtered: gpd.GeoDataFrame,
    regions: gpd.GeoDataFrame,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:  # pyright: ignore[reportInvalidTypeForm]
    polygon_to_regions_map = (
        polygons_filtered.overlay(
            regions[["region_id", "geometry"]],
            how="intersection",
        )
        .assign(area=lambda df: df["geometry"].area)
        .query("area > (0.3 * 1e6)")
        .groupby("polygon_id")["region_id"]
        .apply(lambda x: list(set(x.to_list())))
    )

    polygon_country_count = (
        polygon_to_regions_map.explode()
        .to_frame()
        .reset_index(names="polygon_id")
        .merge(regions, on="region_id")
        .groupby("polygon_id")["GID_0"]
        .nunique()
    )

    idx_in_one_country = polygon_country_count[polygon_country_count == 1].index  # noqa: F841
    idx_in_two_countries = polygon_country_count[polygon_country_count == 2].index  # noqa: F841

    polygons_filtered = polygons_filtered.assign(
        regions=lambda df: df["polygon_id"].map(polygon_to_regions_map.to_dict()),
    )

    if "polygons_one_country" in context.selected_output_names:
        yield dg.Output(  # pyright: ignore[reportReturnType]
            polygons_filtered.query("polygon_id in @idx_in_one_country"),
            output_name="polygons_one_country",
        )

    if "polygons_two_countries" in context.selected_output_names:
        yield dg.Output(  # pyright: ignore[reportReturnType]
            polygons_filtered.query("polygon_id in @idx_in_two_countries"),
            output_name="polygons_two_countries",
        )


@dg.asset(
    key=["polygons", "joined_with_gadm"],
    ins={
        "polygons_one_country": dg.AssetIn(key=["polygons", "one_country"]),
        "polygons_two_countries": dg.AssetIn(key=["polygons", "two_countries"]),
        "regions": dg.AssetIn(["regions", "lowest_level"]),
    },
    partitions_def=year_and_threshold_partitions,
    io_manager_key="geodataframe_manager",
    group_name="polygons",
)
def polygons_joined_with_gadm(
    polygons_one_country: gpd.GeoDataFrame,
    polygons_two_countries: gpd.GeoDataFrame,
    regions: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    agg_map = {
        "GID_0": reduce_to_set_and_join,
        "GID_1": reduce_to_set_and_join,
        "GID_2": reduce_to_set_and_join,
        "GID_3": reduce_to_set_and_join,
        "GID_4": reduce_to_set_and_join,
        "NAME_0": reduce_to_set_and_join,
        "NAME_1": reduce_to_set_and_join,
        "NAME_2": reduce_to_set_and_join,
        "NAME_3": reduce_to_set_and_join,
        "NAME_4": reduce_to_set_and_join,
        "geometry": "first",
    }

    polygons_one_country_exploded = (
        polygons_one_country.assign(
            regions=lambda df: df["regions"].apply(literal_eval),
        )
        .explode("regions")
        .merge(
            regions.drop(columns=["geometry"]),
            left_on="regions",
            right_on="region_id",
        )
    )
    polygons_two_countries_exploded = (
        polygons_two_countries.assign(
            regions=lambda df: df["regions"].apply(literal_eval),
        )
        .explode("regions")
        .merge(
            regions.drop(columns=["geometry"]),
            left_on="regions",
            right_on="region_id",
        )
    )

    processed_one = pd.DataFrame(
        polygons_one_country_exploded.groupby("polygon_id").agg(
            agg_map,
        ),
    )
    processed_two = pd.DataFrame(
        (
            polygons_two_countries_exploded.assign(
                NAME_1=lambda df: df.apply(
                    lambda row: append_country_name(row["NAME_1"], row["GID_0"]),
                    axis=1,
                ),
                NAME_2=lambda df: df.apply(
                    lambda row: append_country_name(row["NAME_2"], row["GID_0"]),
                    axis=1,
                ),
                NAME_3=lambda df: df.apply(
                    lambda row: append_country_name(row["NAME_3"], row["GID_0"]),
                    axis=1,
                ),
                NAME_4=lambda df: df.apply(
                    lambda row: append_country_name(row["NAME_4"], row["GID_0"]),
                    axis=1,
                ),
            )
        )
        .groupby("polygon_id")
        .agg(agg_map),
    )

    return gpd.GeoDataFrame(
        pd.concat(
            [processed_one, processed_two],
        )
        .sort_index()
        .reset_index(),
        crs="ESRI:54009",
        geometry="geometry",
    )


def merge_single_countries_with_features(
    polygons_single: gpd.GeoDataFrame,
    features: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    joined = (
        polygons_single[["polygon_id", "GID_0", "geometry"]]
        .sjoin(
            features[["feature_id", "place", "GID_0", "geometry"]],
            how="inner",
            predicate="contains",
            lsuffix="polygon",
            rsuffix="feature",
        )
        .drop(columns=["index_feature"])
        .query("GID_0_polygon == GID_0_feature")
    )
    top_place = joined.groupby("polygon_id")["place"].min()

    feature_id_to_name = (
        features.set_index("feature_id")
        .assign(name=lambda df: df["name"] + " [" + df.index.astype(str) + "]")["name"]
        .to_dict()
    )

    polygon_to_features_map = (
        top_place.reset_index()
        .merge(joined, on=["polygon_id", "place"], how="inner")
        .groupby("polygon_id")["feature_id"]
        .apply(list)
        .reset_index()
        .assign(
            feature_name=lambda df: df["feature_id"].apply(
                lambda feature_ids: "+".join(
                    [feature_id_to_name[fid] for fid in feature_ids],
                ),
            ),
        )
        .drop(columns=["feature_id"])
    )
    return polygons_single.merge(polygon_to_features_map, on="polygon_id", how="left")


def merge_double_countries_with_features(
    polygons_double: gpd.GeoDataFrame,
    features: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    joined = (
        polygons_double.assign(country_list=lambda df: df["GID_0"].str.split("+"))
        .explode("country_list")
        .assign(
            duplicate_id=lambda df: df.groupby("polygon_id").cumcount(),
            modified_polygon_id=lambda df: df["polygon_id"]
            + "_"
            + df["duplicate_id"].astype(str),
        )
        .drop(columns=["duplicate_id"])
        .sjoin(
            features[["feature_id", "place", "GID_0", "geometry"]],
            how="inner",
            predicate="contains",
            lsuffix="polygon",
            rsuffix="feature",
        )
        .drop(columns=["index_feature"])
        .query("country_list == GID_0_feature")
    )

    top_place = joined.groupby("modified_polygon_id")["place"].min()

    feature_id_to_name = (
        features.set_index("feature_id")
        .assign(
            name=lambda df: df["name"]
            + " ("
            + df["GID_0"]
            + ") ["
            + df.index.astype(str)
            + "]",
        )["name"]
        .to_dict()
    )

    polygon_to_features_map = (
        top_place.reset_index()
        .merge(joined, on=["modified_polygon_id", "place"], how="inner")
        .groupby("modified_polygon_id")["feature_id"]
        .apply(list)
        .reset_index()
        .assign(
            feature_name=lambda df: df["feature_id"].apply(
                lambda feature_ids: "+".join(
                    [feature_id_to_name[fid] for fid in feature_ids],
                ),
            ),
            polygon_id=lambda df: df["modified_polygon_id"].str.rsplit("_", n=1).str[0],
        )
        .drop(columns=["feature_id"])
        .groupby("polygon_id", as_index=False)
        .agg({"feature_name": "+".join})
    )

    return polygons_double.merge(polygon_to_features_map, on="polygon_id", how="left")


@dg.asset(
    key=["polygons", "joined_with_features"],
    ins={
        "polygons_joined_with_gadm": dg.AssetIn(key=["polygons", "joined_with_gadm"]),
        "features": dg.AssetIn(["features", "filtered_places"]),
    },
    partitions_def=year_and_threshold_partitions,
    io_manager_key="geodataframe_manager",
    group_name="polygons",
)
def polygons_joined_with_features(
    polygons_joined_with_gadm: gpd.GeoDataFrame,
    features: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    features = features.assign(
        place=lambda df: pd.Categorical(
            df["place"],
            ["city", "town", "village", "hamlet"],
            ordered=True,
        ),
    )
    polygons_joined_with_gadm = polygons_joined_with_gadm.assign(
        country_list=lambda df: df["GID_0"].str.split("+"),
    )

    polygons_single = polygons_joined_with_gadm.query(
        "country_list.str.len() == 1",
    ).drop(
        columns=["country_list"],
    )
    polygons_double = polygons_joined_with_gadm.query(
        "country_list.str.len() == 2",
    ).drop(
        columns=["country_list"],
    )

    processed_single = merge_single_countries_with_features(
        polygons_single,
        features,
    )
    processed_double = merge_double_countries_with_features(
        polygons_double,
        features,
    )
    return (
        gpd.GeoDataFrame(
            pd.concat([processed_single, processed_double], ignore_index=True),
        )
        .sort_values("polygon_id")
        .rename(columns={"feature_name": "name"})
    )


@dg.asset(
    key=["polygons", "joined_with_geocoding"],
    ins={
        "polygons_joined_with_features": dg.AssetIn(
            key=["polygons", "joined_with_features"],
        ),
        "geocoding_points": dg.AssetIn(key=["geocoding", "download"]),
    },
    partitions_def=year_and_threshold_partitions,
    io_manager_key="geodataframe_manager",
    group_name="polygons",
)
def polygons_joined_with_geocoding(
    polygons_joined_with_features: gpd.GeoDataFrame,
    geocoding_points: pd.DataFrame,
) -> gpd.GeoDataFrame:
    polygons_without_names = polygons_joined_with_features.query("name.isna()")
    joined = pd.concat(
        [
            polygons_without_names["polygon_id"].reset_index(drop=True),
            geocoding_points["name"].reset_index(drop=True),
        ],
        axis=1,
    ).dropna(subset=["name"])
    return (
        polygons_joined_with_features.merge(
            joined,
            on="polygon_id",
            how="left",
            suffixes=("", "_new"),
        )
        .assign(
            name_new=lambda df: df["name_new"] + " [geocoding]",
            name=lambda df: df["name"].fillna(df["name_new"]),
        )
        .drop(columns=["name_new"])
        .sort_values("polygon_id")
    )


@dg.asset(
    key=["polygons", "renamed_with_nearby"],
    ins={
        "polygons_joined_with_geocoding": dg.AssetIn(
            key=["polygons", "joined_with_geocoding"],
        ),
        "features": dg.AssetIn(["features", "filtered_places"]),
    },
    partitions_def=year_and_threshold_partitions,
    io_manager_key="geodataframe_manager",
    group_name="polygons",
)
def polygons_renamed_with_nearby(
    polygons_joined_with_geocoding: gpd.GeoDataFrame,
    features: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    df_polygons_unnamed = polygons_joined_with_geocoding.query("name.isna()")

    missing_polygons = df_polygons_unnamed.filter(["polygon_id", "geometry"])
    named: list[pd.DataFrame] = []
    found_indices = []

    for level in ["city", "town", "village", "hamlet"]:
        df_level = features.query(f"place == '{level}'").filter(["name", "geometry"])
        joined = (
            missing_polygons.sjoin_nearest(df_level, how="inner", distance_col="dist")
            .query("dist < 10000")
            .drop(columns=["dist", "index_right"])
        )
        named.append(joined)
        found_indices.extend(joined.index.unique().to_list())
        missing_polygons = missing_polygons.drop(index=found_indices, errors="ignore")

    named_df = pd.concat(named).assign(name=lambda df: "Cerca de " + df["name"])

    return (
        polygons_joined_with_geocoding.merge(
            named_df.drop(columns=["geometry"]),
            how="left",
            on="polygon_id",
            suffixes=("", "_new"),
        )
        .assign(name=lambda df: df["name"].fillna(df["name_new"]))
        .drop(columns=["name_new"])
    )


@dg.asset(
    key=["polygons", "unique_name"],
    ins={
        "polygons_renamed_with_nearby": dg.AssetIn(
            key=["polygons", "renamed_with_nearby"],
        ),
        "features": dg.AssetIn(["features", "filtered_places"]),
    },
    partitions_def=year_and_threshold_partitions,
    io_manager_key="geodataframe_manager",
    group_name="polygons",
)
def polygons_unique_name(
    polygons_renamed_with_nearby: gpd.GeoDataFrame,
    features: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    name_count = polygons_renamed_with_nearby["name"].apply(
        lambda x: len(x.split("+")) if not np.isnan(x) else 0,
    )
    single_names = polygons_renamed_with_nearby.loc[name_count == 1, "name"]
    multiple_names = polygons_renamed_with_nearby.loc[name_count > 1, "name"]


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


@dg.op(out=dg.Out(io_manager_key="geodataframe_manager"))
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


@dg.graph_asset(
    key=["polygons", "population"],
    ins={
        "polygons_renamed_with_nearby": dg.AssetIn(
            key=["polygons", "unique_name"],
        ),
    },
    partitions_def=year_and_threshold_partitions,
    group_name="polygons",
)
def polygons_population(
    polygons_renamed_with_nearby: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    polygons = add_total_pop(polygons_renamed_with_nearby)
    return add_smod_pop(polygons)
