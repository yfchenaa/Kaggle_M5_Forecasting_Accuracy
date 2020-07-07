import numpy as np
import pandas as pd
import warnings
import gc


def create_lag_feats(data, recursive=False):
    vars_list = ['revenue', 'sales', 'sell_price']
    if recursive:
        lag_day_list = [7]
    else:
        lag_day_list = [7, 28]
    df = data.copy(deep=True)
    del data
    gc.collect()
    for var in vars_list:
        lag_cols = ["id_" + str(var) + "_lag_" + str(lag) for lag in lag_day_list]

        for lag, lag_col in zip(lag_day_list, lag_cols):
            df[lag_col] = df[["id", var]].groupby("id")[var].shift(lag)

    return df


def create_lag_mean_feats(data, recursive=False):
    df = data.copy(deep=True)
    del data
    gc.collect()
    agg_func_list = ['mean', 'median']
    single_list = ['id']

    if recursive:
        shift_day_list = [7]
    else:
        shift_day_list = [7, 28]

    rolling_day_list = [7, 28, 90]

    for agg_func in agg_func_list:
        print('get_' + agg_func + '_feats...')
        if agg_func == 'median':
            target_list = ['sell_price']
        else:
            target_list = ['sales', 'revenue']
        for var in single_list:
            for target in target_list:
                for shift_day in shift_day_list:
                    for rolling_day in rolling_day_list:
                        new_col_name = target + '_per_' + var + '_lag_' + str(shift_day) + '_r_' + str(
                            rolling_day) + '_' + agg_func
                        print('generating ' + new_col_name)
                        sub_feats = [target, var]
                        # print(target,var)
                        df[new_col_name] = df[sub_feats].groupby(var)[target].transform(
                            lambda x: x.shift(shift_day).rolling(rolling_day).agg(agg_func))

    print('get deviation feats...')
    df['sales_per_id_avg_dev_shift7_d7_28'] = (df['sales_per_id_lag_7_r_7_mean'] - df[
        'sales_per_id_lag_7_r_28_mean'])/df['sales_per_id_lag_7_r_28_mean']
    df['sales_per_id_avg_dev_shift7_d7_90'] = (df['sales_per_id_lag_7_r_7_mean'] - df[
        'sales_per_id_lag_7_r_90_mean'])/df['sales_per_id_lag_7_r_90_mean']

    df['revenue_per_id_avg_dev_shift7_d7_28'] = (df['revenue_per_id_lag_7_r_7_mean'] - df[
        'revenue_per_id_lag_7_r_28_mean'])/df['revenue_per_id_lag_7_r_28_mean']
    df['revenue_per_id_avg_dev_shift7_d7_90'] = (df['revenue_per_id_lag_7_r_7_mean'] - df[
        'revenue_per_id_lag_7_r_90_mean'])/df['revenue_per_id_lag_7_r_90_mean']

    df['sell_price_per_id_avg_dev_shift7_d7_28'] = (df['sell_price_per_id_lag_7_r_7_median'] - df[
        'sell_price_per_id_lag_7_r_28_median'])/df['sell_price_per_id_lag_7_r_28_median']
    df['sell_price_per_id_avg_dev_shift7_d7_90'] = (df['sell_price_per_id_lag_7_r_7_median'] - df[
        'sell_price_per_id_lag_7_r_90_median'])/df['sell_price_per_id_lag_7_r_90_median']
    return df
