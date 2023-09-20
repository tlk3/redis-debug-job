import os
from celery import group
from celery.canvas import Signature

from celery_app import app as celery_app
from celery_app import write_cache

__all__ = ('celery_app',)


if __name__ == '__main__':
    # full_df = pd.read_csv('1_min_all.csv')
    # dfs_by_symbol = {name: group for name, group in full_df.groupby('symbol')}

    all_files = os.listdir('csv')
    symbols_from_dir = [filename.replace('.csv', '') for filename in all_files if filename.endswith('.csv')]

    signatures: list[Signature] = []
    for symbol in symbols_from_dir:
        symbol_signature = write_cache.si(symbol)
        signatures.append(symbol_signature)
    celery_group = group(signatures)
    celery_group.delay()
