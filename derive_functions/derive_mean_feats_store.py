import pandas as pd
import gc
import warnings
warnings.filterwarnings('ignore')


def get_df_store(df_train):
    use_cols = ['wday', 'store_id', 'date', 'sales', 'revenue', 'month', 'quarter', 'is_religious_holiday',
                'is_national_holiday', 'is_cultural_holiday', 'is_sporting_holiday', 'snap',
                'is_weekend', 'is_quarter_end']
    data_train = df_train[use_cols].copy(deep=True)
    del df_train
    gc.collect()

    # 按照日期进行sale和revenue的加总后再计算均值
    use_cols_2 = ['wday', 'date', 'month', 'quarter', 'is_religious_holiday',
                  'is_national_holiday', 'is_cultural_holiday', 'is_sporting_holiday', 'snap',
                  'is_weekend', 'is_quarter_end']
    data_date = data_train[use_cols_2].drop_duplicates('date')
    data_train_store_sum = data_train.groupby(['store_id', 'date'], as_index=False)['sales', 'revenue'].sum()
    data_train = pd.merge(data_date, data_train_store_sum, on='date', how='right')
    del data_date, data_train_store_sum
    gc.collect()

    # 每个store不加条件的均值
    df_store_mean = data_train.groupby('store_id', as_index=False)['sales', 'revenue'].mean()
    df_store_mean.rename(columns={'sales': 'avg_sales_per_store', 'revenue': 'avg_revenue_per_store'}, inplace=True)

    # 每周每天的销量和销售额均值
    df_store_wday1 = data_train[data_train['wday'] == 1].groupby('store_id', as_index=False)['sales', 'revenue'].mean()
    df_store_wday1.rename(columns={'sales': 'avg_sales_per_store_wday1',
                                  'revenue': 'avg_revenue_per_store_wday1'}, inplace=True)
    df_store = pd.merge(df_store_mean, df_store_wday1, on='store_id', how='left')
    del df_store_wday1
    gc.collect()

    df_store_wday2 = data_train[data_train['wday'] == 2].groupby('store_id', as_index=False)['sales', 'revenue'].mean()
    df_store_wday2.rename(columns={'sales': 'avg_sales_per_store_wday2',
                                  'revenue': 'avg_revenue_per_store_wday2'}, inplace=True)
    df_store = pd.merge(df_store, df_store_wday2, on='store_id', how='left')
    del df_store_wday2
    gc.collect()

    df_store_wday3 = data_train[data_train['wday'] == 3].groupby('store_id', as_index=False)['sales', 'revenue'].mean()
    df_store_wday3.rename(columns={'sales': 'avg_sales_per_store_wday3',
                                  'revenue': 'avg_revenue_per_store_wday3'}, inplace=True)
    df_store = pd.merge(df_store, df_store_wday3, on='store_id', how='left')
    del df_store_wday3
    gc.collect()

    df_store_wday4 = data_train[data_train['wday'] == 4].groupby('store_id', as_index=False)['sales', 'revenue'].mean()
    df_store_wday4.rename(columns={'sales': 'avg_sales_per_store_wday4',
                                  'revenue': 'avg_revenue_per_store_wday4'}, inplace=True)
    df_store = pd.merge(df_store, df_store_wday4, on='store_id', how='left')
    del df_store_wday4
    gc.collect()

    df_store_wday5 = data_train[data_train['wday'] == 5].groupby('store_id', as_index=False)['sales', 'revenue'].mean()
    df_store_wday5.rename(columns={'sales': 'avg_sales_per_store_wday5',
                                  'revenue': 'avg_revenue_per_store_wday5'}, inplace=True)
    df_store = pd.merge(df_store, df_store_wday5, on='store_id', how='left')
    del df_store_wday5
    gc.collect()

    df_store_wday6 = data_train[data_train['wday'] == 6].groupby('store_id', as_index=False)['sales', 'revenue'].mean()
    df_store_wday6.rename(columns={'sales': 'avg_sales_per_store_wday6',
                                  'revenue': 'avg_revenue_per_store_wday6'}, inplace=True)
    df_store = pd.merge(df_store, df_store_wday6, on='store_id', how='left')
    del df_store_wday6
    gc.collect()

    df_store_wday7 = data_train[data_train['wday'] == 7].groupby('store_id', as_index=False)['sales', 'revenue'].mean()
    df_store_wday7.rename(columns={'sales': 'avg_sales_per_store_wday7',
                                  'revenue': 'avg_revenue_per_store_wday7'}, inplace=True)
    df_store = pd.merge(df_store, df_store_wday7, on='store_id', how='left')
    del df_store_wday7
    gc.collect()

    # 每个月的销量和销售额均值
    df_store_month1 = data_train[data_train['month'] == 1].groupby('store_id', as_index=False)['sales', 'revenue'].mean()
    df_store_month1.rename(columns={'sales': 'avg_sales_per_store_month1',
                                   'revenue': 'avg_revenue_per_store_month1'}, inplace=True)
    df_store = pd.merge(df_store, df_store_month1, on='store_id', how='left')
    del df_store_month1
    gc.collect()

    df_store_month2 = data_train[data_train['month'] == 2].groupby('store_id', as_index=False)['sales', 'revenue'].mean()
    df_store_month2.rename(columns={'sales': 'avg_sales_per_store_month2',
                                   'revenue': 'avg_revenue_per_store_month2'}, inplace=True)
    df_store = pd.merge(df_store, df_store_month2, on='store_id', how='left')
    del df_store_month2
    gc.collect()

    df_store_month3 = data_train[data_train['month'] == 3].groupby('store_id', as_index=False)['sales', 'revenue'].mean()
    df_store_month3.rename(columns={'sales': 'avg_sales_per_store_month3',
                                   'revenue': 'avg_revenue_per_store_month3'}, inplace=True)
    df_store = pd.merge(df_store, df_store_month3, on='store_id', how='left')
    del df_store_month3
    gc.collect()

    df_store_month4 = data_train[data_train['month'] == 4].groupby('store_id', as_index=False)['sales', 'revenue'].mean()
    df_store_month4.rename(columns={'sales': 'avg_sales_per_store_month4',
                                   'revenue': 'avg_revenue_per_store_month4'}, inplace=True)
    df_store = pd.merge(df_store, df_store_month4, on='store_id', how='left')
    del df_store_month4
    gc.collect()

    df_store_month5 = data_train[data_train['month'] == 5].groupby('store_id', as_index=False)['sales', 'revenue'].mean()
    df_store_month5.rename(columns={'sales': 'avg_sales_per_store_month5',
                                   'revenue': 'avg_revenue_per_store_month5'}, inplace=True)
    df_store = pd.merge(df_store, df_store_month5, on='store_id', how='left')
    del df_store_month5
    gc.collect()

    df_store_month6 = data_train[data_train['month'] == 6].groupby('store_id', as_index=False)['sales', 'revenue'].mean()
    df_store_month6.rename(columns={'sales': 'avg_sales_per_store_month6',
                                   'revenue': 'avg_revenue_per_store_month6'}, inplace=True)
    df_store = pd.merge(df_store, df_store_month6, on='store_id', how='left')
    del df_store_month6
    gc.collect()

    df_store_month7 = data_train[data_train['month'] == 7].groupby('store_id', as_index=False)['sales', 'revenue'].mean()
    df_store_month7.rename(columns={'sales': 'avg_sales_per_store_month7',
                                   'revenue': 'avg_revenue_per_store_month7'}, inplace=True)
    df_store = pd.merge(df_store, df_store_month7, on='store_id', how='left')
    del df_store_month7
    gc.collect()

    df_store_month8 = data_train[data_train['month'] == 8].groupby('store_id', as_index=False)['sales', 'revenue'].mean()
    df_store_month8.rename(columns={'sales': 'avg_sales_per_store_month8',
                                   'revenue': 'avg_revenue_per_store_month8'}, inplace=True)
    df_store = pd.merge(df_store, df_store_month8, on='store_id', how='left')
    del df_store_month8
    gc.collect()

    df_store_month9 = data_train[data_train['month'] == 9].groupby('store_id', as_index=False)['sales', 'revenue'].mean()
    df_store_month9.rename(columns={'sales': 'avg_sales_per_store_month9',
                                   'revenue': 'avg_revenue_per_store_month9'}, inplace=True)
    df_store = pd.merge(df_store, df_store_month9, on='store_id', how='left')
    del df_store_month9
    gc.collect()

    df_store_month10 = data_train[data_train['month'] == 10].groupby('store_id', as_index=False)[
        'sales', 'revenue'].mean()
    df_store_month10.rename(columns={'sales': 'avg_sales_per_store_month10',
                                    'revenue': 'avg_revenue_per_store_month10'}, inplace=True)
    df_store = pd.merge(df_store, df_store_month10, on='store_id', how='left')
    del df_store_month10
    gc.collect()

    df_store_month11 = data_train[data_train['month'] == 11].groupby('store_id', as_index=False)[
        'sales', 'revenue'].mean()
    df_store_month11.rename(columns={'sales': 'avg_sales_per_store_month11',
                                    'revenue': 'avg_revenue_per_store_month11'}, inplace=True)
    df_store = pd.merge(df_store, df_store_month11, on='store_id', how='left')
    del df_store_month11
    gc.collect()

    df_store_month12 = data_train[data_train['month'] == 12].groupby('store_id', as_index=False)[
        'sales', 'revenue'].mean()
    df_store_month12.rename(columns={'sales': 'avg_sales_per_store_month12',
                                    'revenue': 'avg_revenue_per_store_month12'}, inplace=True)
    df_store = pd.merge(df_store, df_store_month12, on='store_id', how='left')
    del df_store_month12
    gc.collect()

    # 每个季度销售额和销量的均值
    df_store_qtr1 = data_train[data_train['quarter'] == 1].groupby('store_id', as_index=False)['sales', 'revenue'].mean()
    df_store_qtr1.rename(columns={'sales': 'avg_sales_per_store_qtr1',
                                 'revenue': 'avg_revenue_per_store_qtr1'}, inplace=True)
    df_store = pd.merge(df_store, df_store_qtr1, on='store_id', how='left')
    del df_store_qtr1
    gc.collect()

    df_store_qtr2 = data_train[data_train['quarter'] == 2].groupby('store_id', as_index=False)['sales', 'revenue'].mean()
    df_store_qtr2.rename(columns={'sales': 'avg_sales_per_store_qtr2',
                                 'revenue': 'avg_revenue_per_store_qtr2'}, inplace=True)
    df_store = pd.merge(df_store, df_store_qtr2, on='store_id', how='left')
    del df_store_qtr2
    gc.collect()

    df_store_qtr3 = data_train[data_train['quarter'] == 3].groupby('store_id', as_index=False)['sales', 'revenue'].mean()
    df_store_qtr3.rename(columns={'sales': 'avg_sales_per_store_qtr3',
                                 'revenue': 'avg_revenue_per_store_qtr3'}, inplace=True)
    df_store = pd.merge(df_store, df_store_qtr3, on='store_id', how='left')
    del df_store_qtr3
    gc.collect()

    df_store_qtr4 = data_train[data_train['quarter'] == 4].groupby('store_id', as_index=False)['sales', 'revenue'].mean()
    df_store_qtr4.rename(columns={'sales': 'avg_sales_per_store_qtr4',
                                 'revenue': 'avg_revenue_per_store_qtr4'}, inplace=True)
    df_store = pd.merge(df_store, df_store_qtr4, on='store_id', how='left')
    del df_store_qtr4
    gc.collect()

    # 在特定holiday的均值
    df_store_religious_holi_mean = \
        data_train[data_train['is_religious_holiday'] == 1].groupby('store_id', as_index=False)[
            'sales', 'revenue'].mean()
    df_store_religious_holi_mean.rename(columns={'sales': 'avg_sales_per_store_on_religious_holi',
                                                'revenue': 'avg_revenue_per_store_on_religious_holi'}, inplace=True)
    df_store = pd.merge(df_store, df_store_religious_holi_mean, on='store_id', how='left')
    del df_store_religious_holi_mean
    gc.collect()

    df_store_national_holi_mean = data_train[data_train['is_national_holiday'] == 1].groupby('store_id', as_index=False)[
        'sales', 'revenue'].mean()
    df_store_national_holi_mean.rename(columns={'sales': 'avg_sales_per_store_on_national_holi',
                                               'revenue': 'avg_revenue_per_store_on_national_holi'}, inplace=True)
    df_store = pd.merge(df_store, df_store_national_holi_mean, on='store_id', how='left')
    del df_store_national_holi_mean
    gc.collect()

    df_store_cultural_holi_mean = data_train[data_train['is_cultural_holiday'] == 1].groupby('store_id', as_index=False)[
        'sales', 'revenue'].mean()
    df_store_cultural_holi_mean.rename(columns={'sales': 'avg_sales_per_store_on_cultural_holi',
                                               'revenue': 'avg_revenue_per_store_on_cultural_holi'}, inplace=True)
    df_store = pd.merge(df_store, df_store_cultural_holi_mean, on='store_id', how='left')
    del df_store_cultural_holi_mean
    gc.collect()

    df_store_sporting_holi_mean = data_train[data_train['is_sporting_holiday'] == 1].groupby('store_id', as_index=False)[
        'sales', 'revenue'].mean()
    df_store_sporting_holi_mean.rename(columns={'sales': 'avg_sales_per_store_on_sporting_holi',
                                               'revenue': 'avg_revenue_per_store_on_sporting_holi'}, inplace=True)
    df_store = pd.merge(df_store, df_store_sporting_holi_mean, on='store_id', how='left')
    del df_store_sporting_holi_mean
    gc.collect()

    ##——————————————————————————————————————————
    # snap对每一个store_id的影响
    df_store_id_snap_mean = data_train.groupby(['store_id', 'snap'], as_index=False)['revenue', 'sales'].mean()
    df_store_id_snap_mean.rename(
        columns={'sales': 'avg_sales_per_store_id_snap', 'revenue': 'avg_revenue_per_store_id_snap'},
        inplace=True)

    # 是否周末对每一个store_id的影响
    df_store_id_weekend_mean = data_train.groupby(['store_id', 'is_weekend'], as_index=False)['revenue', 'sales'].mean()
    df_store_id_weekend_mean.rename(
        columns={'sales': 'avg_sales_per_store_id_weekend', 'revenue': 'avg_revenue_per_store_id_weekend'}, inplace=True)

    # 是否季末对每一个store_id的影响
    df_store_id_qtr_end_mean = data_train.groupby(['store_id', 'is_quarter_end'], as_index=False)[
        'revenue', 'sales'].mean()
    df_store_id_qtr_end_mean.rename(
        columns={'sales': 'avg_sales_per_store_id_qtr_end', 'revenue': 'avg_revenue_per_store_id_qtr_end'}, inplace=True)

    return df_store, df_store_id_snap_mean, df_store_id_weekend_mean, df_store_id_qtr_end_mean
