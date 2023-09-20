import pandas as pd
from celery import Celery
from redis_timeseries import stockcache

app = Celery('redis-debug', broker='redis://127.0.0.1:6379/5')


@app.task
def write_cache(symbol):
    df = pd.read_csv(f'csv/{symbol}.csv')
    # print(df)
    stockcache.write_df('stock', symbol, df)
