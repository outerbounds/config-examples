
def benchmark(parquets):
    import polars as pl
    df = pl.concat([pl.read_parquet(parq) for parq in parquets])
    result = (
        df
        .with_columns(
            df["pickup_at"].dt.hour().alias("hour")
        )
        .group_by("hour")
        .agg(pl.col("total_amount").sum().alias("total_amount_sum"))
        .sort('hour')
    )
    return list(map(int, result['total_amount_sum']))