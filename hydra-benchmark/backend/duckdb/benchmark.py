
def benchmark(parquets):
    import duckdb
    con = duckdb.connect()
    parqs = ','.join(f"'{p}'" for p in parquets)
    query = f"""
        SELECT 
            DATE_PART('hour', pickup_at) AS hour,
            SUM(total_amount) AS total_amount_sum
        FROM read_parquet([{parqs}])
        GROUP BY hour
        ORDER BY hour
        """
    return [int(row[1]) for row in con.execute(query).fetchall()]