
def benchmark(parquets):
    import pandas as pd
    df = pd.concat([pd.read_parquet(parq) for parq in parquets], ignore_index=True)
    df['hour'] = df['pickup_at'].dt.hour
    result = df.groupby('hour', as_index=False)['total_amount'].sum().sort_values('hour')
    return list(map(int, result['total_amount']))
