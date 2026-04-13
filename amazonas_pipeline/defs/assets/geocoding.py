import json
import os
import time

import geopandas as gpd
import numpy as np
import pandas as pd
import requests
import shapely

import dagster as dg
from amazonas_pipeline.defs.partitions import year_and_threshold_partitions

iso2_to_iso3 = {
    "AR": "ARG",
    "BO": "BOL",
    "BR": "BRA",
    "CO": "COL",
    "EC": "ECU",
    "GF": "GUF",
    "GY": "GUY",
    "PE": "PER",
    "SR": "SUR",
    "VE": "VEN",
}


def get_centroids(df: gpd.GeoDataFrame) -> dict:
    centroids = (
        df.centroid.to_crs("EPSG:4326")
        .get_coordinates()
        .rename(columns={"x": "lon", "y": "lat"})[["lon", "lat"]]
    )
    centroids["centroid"] = list(zip(centroids["lon"], centroids["lat"], strict=True))
    return centroids["centroid"].to_dict()


def generate_chunks(centroids: dict[int, tuple]) -> tuple[list[list], list[list[dict]]]:
    chunk_size = 100
    n_chunks = int(np.ceil(len(centroids) / chunk_size))

    centroid_keys = list(centroids.keys())
    centroid_vals = list(centroids.values())

    key_chunks = []
    chunks = []
    for i in range(n_chunks):
        key_chunk = centroid_keys[i * 100 : (i + 1) * 100]
        coord_chunk = centroid_vals[i * 100 : (i + 1) * 100]

        chunk = [
            {"coordinates": list(coords), "resultTypes": ["Address", "PopulatedPlace"]}
            for coords in coord_chunk
        ]
        chunks.append(chunk)
        key_chunks.append(key_chunk)

    return key_chunks, chunks


def make_request(chunk: list[dict]) -> requests.Response:
    return requests.post(
        "https://atlas.microsoft.com/reverseGeocode:batch?api-version=2023-06-01",
        json={"batchItems": chunk},
        headers={"subscription-key": os.environ["AZ_MAPS_PK_1"]},
        timeout=600,
    )


def geocode_chunk(key_chunk: list[int], chunk: list[dict]) -> list[dict]:
    retry_count = 0
    while True:
        response = make_request(chunk)
        if response.status_code == 200:
            break
        retry_count += 1
        if retry_count > 5:
            err = "Failed to get a successful response after 5 retries."
            raise RuntimeError(err)
        time.sleep(20)

    content = json.loads(response.content)

    out = []
    for key, item in zip(key_chunk, content["batchItems"], strict=True):
        temp = {}
        features = item["features"]
        if len(features) == 0:
            temp["name"] = None
            temp["geometry"] = None
        else:
            feature = features[0]
            properties = feature["properties"]

            if properties["type"] == "Address":
                name = properties["address"]["addressLine"]
            elif properties["type"] == "PopulatedPlace":
                name = properties["address"]["locality"]
            else:
                err = f"Unexpected feature type: {properties['type']}"
                raise ValueError(err)

            temp["name"] = name
            temp["geometry"] = shapely.geometry.shape(feature["geometry"])

        temp["key"] = key
        out.append(temp)
    return out


@dg.asset(
    key=["geocoding", "download"],
    ins={"polygons": dg.AssetIn(key=["polygons", "joined_with_features"])},
    partitions_def=year_and_threshold_partitions,
    io_manager_key="geodataframe_manager",
    group_name="geocoding",
)
def download_geocoding_points(
    context: dg.AssetExecutionContext,
    polygons: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    year = int(context.multi_partition_key.keys_by_dimension["year"])
    if year != 2020:
        return gpd.GeoDataFrame(
            [],
            columns=["name", "key"],
            geometry=[],
            crs="EPSG:4326",
        )

    polygons = polygons.query("name.isna()")

    centroids = get_centroids(polygons)
    key_chunks, chunks = generate_chunks(centroids)

    out = []
    for i, (key_chunk, chunk) in enumerate(zip(key_chunks, chunks, strict=True)):
        res = geocode_chunk(key_chunk, chunk)
        out.extend(res)

        msg = f"Processed chunk {i + 1} of {len(chunks)}."
        context.log.info(msg)

        time.sleep(10)

    return gpd.GeoDataFrame(pd.DataFrame(out), geometry="geometry", crs="EPSG:4326")
