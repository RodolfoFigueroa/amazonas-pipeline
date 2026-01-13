# Preliminaries

## Packages

[uv](https://github.com/astral-sh/uv) or similar is required. 

After cloning the repository, run

```
uv sync
```

to create the environment and install all packages.

## Environment variables

The following environment variables are required:

* `DATA_PATH`: Directory where all initial data is contained, and where all generated files will be saved in. The structure is explained in the next section.
* `AZ_MAPS_PK_1`: Primary key of a Microsoft Azure Maps account with *Batch Reverse Geocoding* capabilities.
* `DAGSTER_HOME`: Directory where Dagster config, execution logs and related information will be stored. Should point to the `dagster` directory in this repo.

## Directory structure

The `DATA_PATH` directory should have the following structure:

```
DATA_PATH/
└── initial/
    ├── accessibility
    ├── GADM
    └── geofabrik
```

* `GADM` should contain GeoPackage files downloaded from [GADM](https://gadm.org/). The analysis will be performed for all countries found in this directory.

* `geofabrik` should contain `.pbf.` files downloaded from [Geofabrik](https://download.geofabrik.de/). The spatial extent of these files should cover all countries of analysis.

# Execution

## Running

Use

```
uv run dagster dev
```

to start the Dagster webui. 

## Assets

The data pipeline is represented using a *Directed Acyclic Graph*. Each node is called an **asset**, and represents a discrete unit of data. Assets can depend on other assets, which is represented by an edge between them. 

The process of actually calculating an asset is called **materialization**. This can be achieved by right-clicking on an asset in the webui and selecting *Materialize*. In order for an asset to materialize correctly, all of the assets it depends on (called its **upstream**) must be already materialized.

Once an asset has been materialized it will be saved in the output directory. This defaults to `DATA_PATH/generated`. 

## Groups

To ease organization, assets are collected into **groups**. What follows is a high-level description of the function of each of these groups:

* `regions`: Calculating the region of interest and bounding box using the provided GADM polygons.
* `ghsl`: Downloading, cropping and calculating SMOD rasters using configurable parameters, and polygonizing said rasters.
* `cells`: Splitting the obtained polygons into individual cells for further processing.
* `features`: Filtering and processing Geofabrik features to obtain settlement names.
* `polygons`: Processing the obtained settlement polygons, including obtaining their names, population, and historical evolution.
* `sheets`: Converting the final polygons into Excel sheets.