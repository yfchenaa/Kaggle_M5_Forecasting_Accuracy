# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

from  datetime import datetime, timedelta
import gc
import numpy as np, pandas as pd
import lightgbm as lgb


def create_dt(is_train=True, nrows=None, first_day=1200, tr_last=1941, h=28):
    CAL_DTYPES = {"event_name_1": "category", "event_name_2": "category", "event_type_1": "category",
                  "event_type_2": "category", "weekday": "category", 'wm_yr_wk': 'int16', "wday": "int16",
                  "month": "int16", "year": "int16", "snap_CA": "float32", 'snap_TX': 'float32', 'snap_WI': 'float32'}
    PRICE_DTYPES = {"store_id": "category", "item_id": "category", "wm_yr_wk": "int16", "sell_price": "float32"}
    prices = pd.read_csv("data/raw_data/sell_prices.csv", dtype=PRICE_DTYPES)
    #     for col, col_dtype in PRICE_DTYPES.items():
    #         if col_dtype == "category":
    #             prices[col] = prices[col].cat.codes.astype("int16")
    #             prices[col] -= prices[col].min()

    cal = pd.read_csv("data/raw_data/calendar_full.csv", dtype=CAL_DTYPES)
    cal["date"] = pd.to_datetime(cal["date"])
    #     for col, col_dtype in CAL_DTYPES.items():
    #         if col_dtype == "category":
    #             cal[col] = cal[col].cat.codes.astype("int16")
    #             cal[col] -= cal[col].min()

    start_day = max(1 if is_train else tr_last - 200, first_day)
    numcols = [f"d_{day}" for day in range(start_day, tr_last + 1)]
    catcols = ['id', 'item_id', 'dept_id', 'store_id', 'cat_id', 'state_id']
    dtype = {numcol: "float32" for numcol in numcols}
    dtype.update({col: "category" for col in catcols if col != "id"})
    dt = pd.read_csv("data/raw_data/sales_train_evaluation.csv",
                     nrows=nrows, usecols=catcols + numcols, dtype=dtype)

    #     for col in catcols:
    #         if col != "id":
    #             dt[col] = dt[col].cat.codes.astype("int16")
    #             dt[col] -= dt[col].min()

    if not is_train:
        for day in range(tr_last + 1, tr_last + h + 1):
            dt[f"d_{day}"] = np.nan

    dt = pd.melt(dt,
                 id_vars=catcols,
                 value_vars=[col for col in dt.columns if col.startswith("d_")],
                 var_name="d",
                 value_name="sales")

    dt = dt.merge(cal, on="d", copy=False)
    dt = dt.merge(prices, on=["store_id", "item_id", "wm_yr_wk"], copy=False)
    dt = dt.sort_values(by=['id', 'd']).reset_index(drop=True)
    dt['revenue'] = dt['sales'] * dt['sell_price']

    return dt


# helper functions to reduce memory
def reduce_mem_usage(df, verbose=True):
    numerics = ['int16', 'int32', 'int64', 'float16', 'float32', 'float64']
    start_mem = df.memory_usage().sum() / 1024**2
    for col in df.columns:
        col_type = df[col].dtypes
        if col_type in numerics:
            c_min = df[col].min()
            c_max = df[col].max()
            if str(col_type)[:3] == 'int':
                if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                    df[col] = df[col].astype(np.int8)
                elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                    df[col] = df[col].astype(np.int16)
                elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                    df[col] = df[col].astype(np.int32)
                elif c_min > np.iinfo(np.int64).min and c_max < np.iinfo(np.int64).max:
                    df[col] = df[col].astype(np.int64)
            else:
                if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                    df[col] = df[col].astype(np.float16)
                elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                    df[col] = df[col].astype(np.float32)
                else:
                    df[col] = df[col].astype(np.float64)
    end_mem = df.memory_usage().sum() / 1024**2
    if verbose: print('Mem. usage decreased to {:5.2f} Mb ({:.1f}% reduction)'.format(end_mem, 100 * (start_mem - end_mem) / start_mem))
    return df
