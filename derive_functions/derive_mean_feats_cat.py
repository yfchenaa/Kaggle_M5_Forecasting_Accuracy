import pandas as pd
import gc
import warnings

warnings.filterwarnings('ignore')


def get_df_cat(df_train):
    use_cols = ['wday', 'cat_id', 'date', 'sales', 'revenue', 'month', 'quarter', 'is_religious_holiday',
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
    data_train_cat_sum = data_train.groupby(['cat_id', 'date'], as_index=False)['sales', 'revenue'].sum()
    data_train = pd.merge(data_date, data_train_cat_sum, on='date', how='right')
    del data_date, data_train_cat_sum
    gc.collect()

    # 每个cat不加条件的均值
    df_cat_mean = data_train.groupby('cat_id', as_index=False)['sales', 'revenue'].mean()
    df_cat_mean.rename(columns={'sales': 'avg_sales_per_cat', 'revenue': 'avg_revenue_per_cat'}, inplace=True)

    # 每周每天的销量和销售额均值
    df_cat_wday1 = data_train[data_train['wday'] == 1].groupby('cat_id', as_index=False)['sales', 'revenue'].mean()
    df_cat_wday1.rename(columns={'sales': 'avg_sales_per_cat_wday1',
                                 'revenue': 'avg_revenue_per_cat_wday1'}, inplace=True)
    df_cat = pd.merge(df_cat_mean, df_cat_wday1, on='cat_id', how='left')
    del df_cat_wday1
    gc.collect()

    df_cat_wday2 = data_train[data_train['wday'] == 2].groupby('cat_id', as_index=False)['sales', 'revenue'].mean()
    df_cat_wday2.rename(columns={'sales': 'avg_sales_per_cat_wday2',
                                 'revenue': 'avg_revenue_per_cat_wday2'}, inplace=True)
    df_cat = pd.merge(df_cat, df_cat_wday2, on='cat_id', how='left')
    del df_cat_wday2
    gc.collect()

    df_cat_wday3 = data_train[data_train['wday'] == 3].groupby('cat_id', as_index=False)['sales', 'revenue'].mean()
    df_cat_wday3.rename(columns={'sales': 'avg_sales_per_cat_wday3',
                                 'revenue': 'avg_revenue_per_cat_wday3'}, inplace=True)
    df_cat = pd.merge(df_cat, df_cat_wday3, on='cat_id', how='left')
    del df_cat_wday3
    gc.collect()

    df_cat_wday4 = data_train[data_train['wday'] == 4].groupby('cat_id', as_index=False)['sales', 'revenue'].mean()
    df_cat_wday4.rename(columns={'sales': 'avg_sales_per_cat_wday4',
                                 'revenue': 'avg_revenue_per_cat_wday4'}, inplace=True)
    df_cat = pd.merge(df_cat, df_cat_wday4, on='cat_id', how='left')
    del df_cat_wday4
    gc.collect()

    df_cat_wday5 = data_train[data_train['wday'] == 5].groupby('cat_id', as_index=False)['sales', 'revenue'].mean()
    df_cat_wday5.rename(columns={'sales': 'avg_sales_per_cat_wday5',
                                 'revenue': 'avg_revenue_per_cat_wday5'}, inplace=True)
    df_cat = pd.merge(df_cat, df_cat_wday5, on='cat_id', how='left')
    del df_cat_wday5
    gc.collect()

    df_cat_wday6 = data_train[data_train['wday'] == 6].groupby('cat_id', as_index=False)['sales', 'revenue'].mean()
    df_cat_wday6.rename(columns={'sales': 'avg_sales_per_cat_wday6',
                                 'revenue': 'avg_revenue_per_cat_wday6'}, inplace=True)
    df_cat = pd.merge(df_cat, df_cat_wday6, on='cat_id', how='left')
    del df_cat_wday6
    gc.collect()

    df_cat_wday7 = data_train[data_train['wday'] == 7].groupby('cat_id', as_index=False)['sales', 'revenue'].mean()
    df_cat_wday7.rename(columns={'sales': 'avg_sales_per_cat_wday7',
                                 'revenue': 'avg_revenue_per_cat_wday7'}, inplace=True)
    df_cat = pd.merge(df_cat, df_cat_wday7, on='cat_id', how='left')
    del df_cat_wday7
    gc.collect()

    # 每个月的销量和销售额均值
    df_cat_month1 = data_train[data_train['month'] == 1].groupby('cat_id', as_index=False)['sales', 'revenue'].mean()
    df_cat_month1.rename(columns={'sales': 'avg_sales_per_cat_month1',
                                  'revenue': 'avg_revenue_per_cat_month1'}, inplace=True)
    df_cat = pd.merge(df_cat, df_cat_month1, on='cat_id', how='left')
    del df_cat_month1
    gc.collect()

    df_cat_month2 = data_train[data_train['month'] == 2].groupby('cat_id', as_index=False)['sales', 'revenue'].mean()
    df_cat_month2.rename(columns={'sales': 'avg_sales_per_cat_month2',
                                  'revenue': 'avg_revenue_per_cat_month2'}, inplace=True)
    df_cat = pd.merge(df_cat, df_cat_month2, on='cat_id', how='left')
    del df_cat_month2
    gc.collect()

    df_cat_month3 = data_train[data_train['month'] == 3].groupby('cat_id', as_index=False)['sales', 'revenue'].mean()
    df_cat_month3.rename(columns={'sales': 'avg_sales_per_cat_month3',
                                  'revenue': 'avg_revenue_per_cat_month3'}, inplace=True)
    df_cat = pd.merge(df_cat, df_cat_month3, on='cat_id', how='left')
    del df_cat_month3
    gc.collect()

    df_cat_month4 = data_train[data_train['month'] == 4].groupby('cat_id', as_index=False)['sales', 'revenue'].mean()
    df_cat_month4.rename(columns={'sales': 'avg_sales_per_cat_month4',
                                  'revenue': 'avg_revenue_per_cat_month4'}, inplace=True)
    df_cat = pd.merge(df_cat, df_cat_month4, on='cat_id', how='left')
    del df_cat_month4
    gc.collect()

    df_cat_month5 = data_train[data_train['month'] == 5].groupby('cat_id', as_index=False)['sales', 'revenue'].mean()
    df_cat_month5.rename(columns={'sales': 'avg_sales_per_cat_month5',
                                  'revenue': 'avg_revenue_per_cat_month5'}, inplace=True)
    df_cat = pd.merge(df_cat, df_cat_month5, on='cat_id', how='left')
    del df_cat_month5
    gc.collect()

    df_cat_month6 = data_train[data_train['month'] == 6].groupby('cat_id', as_index=False)['sales', 'revenue'].mean()
    df_cat_month6.rename(columns={'sales': 'avg_sales_per_cat_month6',
                                  'revenue': 'avg_revenue_per_cat_month6'}, inplace=True)
    df_cat = pd.merge(df_cat, df_cat_month6, on='cat_id', how='left')
    del df_cat_month6
    gc.collect()

    df_cat_month7 = data_train[data_train['month'] == 7].groupby('cat_id', as_index=False)['sales', 'revenue'].mean()
    df_cat_month7.rename(columns={'sales': 'avg_sales_per_cat_month7',
                                  'revenue': 'avg_revenue_per_cat_month7'}, inplace=True)
    df_cat = pd.merge(df_cat, df_cat_month7, on='cat_id', how='left')
    del df_cat_month7
    gc.collect()

    df_cat_month8 = data_train[data_train['month'] == 8].groupby('cat_id', as_index=False)['sales', 'revenue'].mean()
    df_cat_month8.rename(columns={'sales': 'avg_sales_per_cat_month8',
                                  'revenue': 'avg_revenue_per_cat_month8'}, inplace=True)
    df_cat = pd.merge(df_cat, df_cat_month8, on='cat_id', how='left')
    del df_cat_month8
    gc.collect()

    df_cat_month9 = data_train[data_train['month'] == 9].groupby('cat_id', as_index=False)['sales', 'revenue'].mean()
    df_cat_month9.rename(columns={'sales': 'avg_sales_per_cat_month9',
                                  'revenue': 'avg_revenue_per_cat_month9'}, inplace=True)
    df_cat = pd.merge(df_cat, df_cat_month9, on='cat_id', how='left')
    del df_cat_month9
    gc.collect()

    df_cat_month10 = data_train[data_train['month'] == 10].groupby('cat_id', as_index=False)[
        'sales', 'revenue'].mean()
    df_cat_month10.rename(columns={'sales': 'avg_sales_per_cat_month10',
                                   'revenue': 'avg_revenue_per_cat_month10'}, inplace=True)
    df_cat = pd.merge(df_cat, df_cat_month10, on='cat_id', how='left')
    del df_cat_month10
    gc.collect()

    df_cat_month11 = data_train[data_train['month'] == 11].groupby('cat_id', as_index=False)[
        'sales', 'revenue'].mean()
    df_cat_month11.rename(columns={'sales': 'avg_sales_per_cat_month11',
                                   'revenue': 'avg_revenue_per_cat_month11'}, inplace=True)
    df_cat = pd.merge(df_cat, df_cat_month11, on='cat_id', how='left')
    del df_cat_month11
    gc.collect()

    df_cat_month12 = data_train[data_train['month'] == 12].groupby('cat_id', as_index=False)[
        'sales', 'revenue'].mean()
    df_cat_month12.rename(columns={'sales': 'avg_sales_per_cat_month12',
                                   'revenue': 'avg_revenue_per_cat_month12'}, inplace=True)
    df_cat = pd.merge(df_cat, df_cat_month12, on='cat_id', how='left')
    del df_cat_month12
    gc.collect()

    # 每个季度销售额和销量的均值
    df_cat_qtr1 = data_train[data_train['quarter'] == 1].groupby('cat_id', as_index=False)['sales', 'revenue'].mean()
    df_cat_qtr1.rename(columns={'sales': 'avg_sales_per_cat_qtr1',
                                'revenue': 'avg_revenue_per_cat_qtr1'}, inplace=True)
    df_cat = pd.merge(df_cat, df_cat_qtr1, on='cat_id', how='left')
    del df_cat_qtr1
    gc.collect()

    df_cat_qtr2 = data_train[data_train['quarter'] == 2].groupby('cat_id', as_index=False)['sales', 'revenue'].mean()
    df_cat_qtr2.rename(columns={'sales': 'avg_sales_per_cat_qtr2',
                                'revenue': 'avg_revenue_per_cat_qtr2'}, inplace=True)
    df_cat = pd.merge(df_cat, df_cat_qtr2, on='cat_id', how='left')
    del df_cat_qtr2
    gc.collect()

    df_cat_qtr3 = data_train[data_train['quarter'] == 3].groupby('cat_id', as_index=False)['sales', 'revenue'].mean()
    df_cat_qtr3.rename(columns={'sales': 'avg_sales_per_cat_qtr3',
                                'revenue': 'avg_revenue_per_cat_qtr3'}, inplace=True)
    df_cat = pd.merge(df_cat, df_cat_qtr3, on='cat_id', how='left')
    del df_cat_qtr3
    gc.collect()

    df_cat_qtr4 = data_train[data_train['quarter'] == 4].groupby('cat_id', as_index=False)['sales', 'revenue'].mean()
    df_cat_qtr4.rename(columns={'sales': 'avg_sales_per_cat_qtr4',
                                'revenue': 'avg_revenue_per_cat_qtr4'}, inplace=True)
    df_cat = pd.merge(df_cat, df_cat_qtr4, on='cat_id', how='left')
    del df_cat_qtr4
    gc.collect()

    # 在特定holiday的均值
    df_cat_religious_holi_mean = data_train[data_train['is_religious_holiday'] == 1].groupby('cat_id', as_index=False)[
        'sales', 'revenue'].mean()
    df_cat_religious_holi_mean.rename(columns={'sales': 'avg_sales_per_cat_on_religious_holi',
                                               'revenue': 'avg_revenue_per_cat_on_religious_holi'}, inplace=True)
    df_cat = pd.merge(df_cat, df_cat_religious_holi_mean, on='cat_id', how='left')
    del df_cat_religious_holi_mean
    gc.collect()

    df_cat_national_holi_mean = data_train[data_train['is_national_holiday'] == 1].groupby('cat_id', as_index=False)[
        'sales', 'revenue'].mean()
    df_cat_national_holi_mean.rename(columns={'sales': 'avg_sales_per_cat_on_national_holi',
                                              'revenue': 'avg_revenue_per_cat_on_national_holi'}, inplace=True)
    df_cat = pd.merge(df_cat, df_cat_national_holi_mean, on='cat_id', how='left')
    del df_cat_national_holi_mean
    gc.collect()

    df_cat_cultural_holi_mean = data_train[data_train['is_cultural_holiday'] == 1].groupby('cat_id', as_index=False)[
        'sales', 'revenue'].mean()
    df_cat_cultural_holi_mean.rename(columns={'sales': 'avg_sales_per_cat_on_cultural_holi',
                                              'revenue': 'avg_revenue_per_cat_on_cultural_holi'}, inplace=True)
    df_cat = pd.merge(df_cat, df_cat_cultural_holi_mean, on='cat_id', how='left')
    del df_cat_cultural_holi_mean
    gc.collect()

    df_cat_sporting_holi_mean = data_train[data_train['is_sporting_holiday'] == 1].groupby('cat_id', as_index=False)[
        'sales', 'revenue'].mean()
    df_cat_sporting_holi_mean.rename(columns={'sales': 'avg_sales_per_cat_on_sporting_holi',
                                              'revenue': 'avg_revenue_per_cat_on_sporting_holi'}, inplace=True)
    df_cat = pd.merge(df_cat, df_cat_sporting_holi_mean, on='cat_id', how='left')
    del df_cat_sporting_holi_mean
    gc.collect()

    ##——————————————————————————————————————————
    # snap对每一个cat_id的影响
    df_cat_id_snap_mean = data_train.groupby(['cat_id', 'snap'], as_index=False)['revenue', 'sales'].mean()
    df_cat_id_snap_mean.rename(
        columns={'sales': 'avg_sales_per_cat_id_snap', 'revenue': 'avg_revenue_per_cat_id_snap'},
        inplace=True)

    # 是否周末对每一个cat_id的影响
    df_cat_id_weekend_mean = data_train.groupby(['cat_id', 'is_weekend'], as_index=False)['revenue', 'sales'].mean()
    df_cat_id_weekend_mean.rename(
        columns={'sales': 'avg_sales_per_cat_id_weekend', 'revenue': 'avg_revenue_per_cat_id_weekend'}, inplace=True)

    # 是否季末对每一个cat_id的影响
    df_cat_id_qtr_end_mean = data_train.groupby(['cat_id', 'is_quarter_end'], as_index=False)[
        'revenue', 'sales'].mean()
    df_cat_id_qtr_end_mean.rename(
        columns={'sales': 'avg_sales_per_cat_id_qtr_end', 'revenue': 'avg_revenue_per_cat_id_qtr_end'}, inplace=True)

    return df_cat, df_cat_id_snap_mean, df_cat_id_weekend_mean, df_cat_id_qtr_end_mean
