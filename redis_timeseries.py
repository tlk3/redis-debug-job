import logging

import pandas as pd
from redis_timeseries_manager import RedisTimeseriesManager

from perf import func_timer
from time import perf_counter

logger = logging.getLogger(__name__)


settings = {
    'host': '127.0.0.1',
    'port': 6379,
    'db': 10,
    'password': None,
}

one_hour = 60 * 60
one_day = 60 * 60 * 24
one_week = one_day * 7
one_month = one_day * 30
one_year = one_day * 365


class BaseTimeseriesCache(RedisTimeseriesManager):
    _name: str
    _lines: list[str]
    _timeframes: dict[str, dict[str, int]]

    @func_timer
    def write_df(self, symbol_type: str, symbol: str, df: pd.DataFrame):
        df['timestamp'] = df['timestamp'] / 1000
        df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
        records = []
        for row in df.values.tolist():
            records.append([int(row[0]), float(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5])])
        self.insert(
            c1=symbol_type,
            c2=symbol,
            data=records,
            create_inplace=True,
            # pipeline=True,
        )
        logger.info(f'cached [{symbol}] ({symbol_type})')

    @func_timer
    def read_df(self, symbol_type: str, symbol: str, timeframe: str = 'raw', **kwargs) -> pd.DataFrame:
        limit = kwargs.get('limit', 5000)
        if timeframe == '1':
            timeframe = 'raw'
        df = self.read(
            c1=symbol_type,
            c2=symbol,
            timeframe=timeframe,
            read_from_last=True,
            limit=limit,
            return_as='df',
            **kwargs
        )[1]
        if isinstance(df, str):
            return pd.DataFrame()
        df['timestamp'] = df['time']
        df['time'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
        df.set_index('time', inplace=True)
        df.rename(
            columns={
                'o': 'open',
                'h': 'high',
                'l': 'low',
                'c': 'close',
                'v': 'volume',
            }, inplace=True)
        return df


class StockTimeseriesCache(BaseTimeseriesCache):
    _name = 'polygon'
    _lines = ['o', 'h', 'l', 'c', 'v']
    _timeframes = {
        'raw': {'retention_secs': one_month},
        '5': {'retention_secs': one_week, 'bucket_size_secs': 60 * 5},
        '15': {'retention_secs': one_week, 'bucket_size_secs': 60 * 15},
        '30': {'retention_secs': one_week, 'bucket_size_secs': 60 * 30},
        'd': {'retention_secs': one_year * 5, 'bucket_size_secs': one_day},
    }

    def _create_rule(
            self, c1: str,
            c2: str,
            line: str,
            timeframe_name: str,
            timeframe_specs: dict,
            source_key: str,
            dest_key: str
    ):
        match line:
            case 'o': aggregation_type = 'first'
            case 'h': aggregation_type = 'max'
            case 'l': aggregation_type = 'min'
            case 'c': aggregation_type = 'last'
            case 'v': aggregation_type = 'sum'
            case _: return
        bucket_size_secs = timeframe_specs['bucket_size_secs']
        self._set_rule(source_key, dest_key, aggregation_type, bucket_size_secs)


stockcache = StockTimeseriesCache(**settings)
