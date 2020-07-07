import pandas as pd
import numpy as np
import gc


def get_deviation_feats(data):
    df = data.copy(deep=True)
    del data
    gc.collect()
    ##——snap————————————————————————————————————————————————
    df['avg_revenue_per_store_snap_dev'] = (df['avg_revenue_per_store_id_snap'] - df['avg_revenue_per_store']) / df[
        'avg_revenue_per_store']
    df['avg_revenue_per_id_snap_dev'] = (df['avg_revenue_per_id_snap'] - df['avg_revenue_per_id']) / df[
        'avg_revenue_per_id']
    df['avg_revenue_per_dept_snap_dev'] = (df['avg_revenue_per_dept_id_snap'] - df['avg_revenue_per_dept']) / df[
        'avg_revenue_per_dept']

    df['avg_sales_per_store_snap_dev'] = (df['avg_sales_per_store_id_snap'] - df['avg_sales_per_store']) / df[
        'avg_sales_per_store']
    df['avg_sales_per_id_snap_dev'] = (df['avg_sales_per_id_snap'] - df['avg_sales_per_id']) / df['avg_sales_per_id']
    df['avg_sales_per_dept_snap_dev'] = (df['avg_sales_per_dept_id_snap'] - df['avg_sales_per_dept']) / df[
        'avg_sales_per_dept']
    ##——weekend————————————————————————————————————————————————
    df['avg_revenue_per_store_weekend_dev'] = (df['avg_revenue_per_store_id_weekend'] - df['avg_revenue_per_store']) / \
                                              df['avg_revenue_per_store']
    df['avg_revenue_per_id_weekend_dev'] = (df['avg_revenue_per_id_weekend'] - df['avg_revenue_per_id']) / df[
        'avg_revenue_per_id']
    df['avg_revenue_per_dept_weekend_dev'] = (df['avg_revenue_per_dept_id_weekend'] - df['avg_revenue_per_dept']) / df[
        'avg_revenue_per_dept']

    df['avg_sales_per_store_weekend_dev'] = (df['avg_sales_per_store_id_weekend'] - df['avg_sales_per_store']) / df[
        'avg_sales_per_store']
    df['avg_sales_per_id_weekend_dev'] = (df['avg_sales_per_id_weekend'] - df['avg_sales_per_id']) / df[
        'avg_sales_per_id']
    df['avg_sales_per_dept_weekend_dev'] = (df['avg_sales_per_dept_id_weekend'] - df['avg_sales_per_dept']) / df[
        'avg_sales_per_dept']
    ##——qtr_end————————————————————————————————————————————————
    df['avg_revenue_per_store_qtr_end_dev'] = (df['avg_revenue_per_store_id_qtr_end'] - df['avg_revenue_per_store']) / \
                                              df['avg_revenue_per_store']
    df['avg_revenue_per_id_qtr_end_dev'] = (df['avg_revenue_per_id_qtr_end'] - df['avg_revenue_per_id']) / df[
        'avg_revenue_per_id']
    df['avg_revenue_per_dept_qtr_end_dev'] = (df['avg_revenue_per_dept_id_qtr_end'] - df['avg_revenue_per_dept']) / df[
        'avg_revenue_per_dept']

    df['avg_sales_per_store_qtr_end_dev'] = (df['avg_sales_per_store_id_qtr_end'] - df['avg_sales_per_store']) / df[
        'avg_sales_per_store']
    df['avg_sales_per_id_qtr_end_dev'] = (df['avg_sales_per_id_qtr_end'] - df['avg_sales_per_id']) / df[
        'avg_sales_per_id']
    df['avg_sales_per_dept_qtr_end_dev'] = (df['avg_sales_per_dept_id_qtr_end'] - df['avg_sales_per_dept']) / df[
        'avg_sales_per_dept']

    return df
