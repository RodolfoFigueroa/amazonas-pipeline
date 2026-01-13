import dagster as dg

year_partitions = dg.StaticPartitionsDefinition(
    [str(year) for year in range(1975, 2021, 5)],
)


combined_threshold_partitions = dg.StaticPartitionsDefinition(
    [
        f"{density}_{total}"
        for density in range(50, 301, 50)
        for total in range(100, 501, 100)
    ],
)


year_and_threshold_partitions = dg.MultiPartitionsDefinition(
    {
        "year": year_partitions,
        "thresholds": combined_threshold_partitions,
    },
)
