import pandas as pd
import gc
import warnings
warnings.filterwarnings('ignore')


def get_df_id(df_train):
    use_cols = ['wday', 'id', 'date', 'sales', 'revenue', 'month', 'quarter', 'is_religious_holiday',
                'is_national_holiday', 'is_cultural_holiday', 'is_sporting_holiday', 'snap',
                'is_weekend', 'is_quarter_end']
    data_train = df_train[use_cols].copy(deep=True)
    del df_train
    gc.collect()

    # 每个id不加条件的均值
    df_id_mean = data_train.groupby('id', as_index=False)['sales', 'revenue'].mean()
    df_id_mean.rename(columns={'sales': 'avg_sales_per_id', 'revenue': 'avg_revenue_per_id'}, inplace=True)

    df_id_median = data_train.groupby('id', as_index=False)['sales', 'revenue'].median()
    df_id_median.rename(columns={'sales': 'median_sales_per_id', 'revenue': 'median_revenue_per_id'}, inplace=True)
    df_id = pd.merge(df_id_mean, df_id_median, on='id', how='left')
    del df_id_median
    gc.collect()

    # 每周每天的销量和销售额均值
    df_id_wday1 = data_train[data_train['wday'] == 1].groupby('id', as_index=False)['sales', 'revenue'].mean()
    df_id_wday1.rename(columns={'sales': 'avg_sales_per_id_wday1',
                                'revenue': 'avg_revenue_per_id_wday1'}, inplace=True)
    df_id = pd.merge(df_id, df_id_wday1, on='id', how='left')
    del df_id_wday1
    gc.collect()

    df_id_wday2 = data_train[data_train['wday'] == 2].groupby('id', as_index=False)['sales', 'revenue'].mean()
    df_id_wday2.rename(columns={'sales': 'avg_sales_per_id_wday2',
                                'revenue': 'avg_revenue_per_id_wday2'}, inplace=True)
    df_id = pd.merge(df_id, df_id_wday2, on='id', how='left')
    del df_id_wday2
    gc.collect()

    df_id_wday3 = data_train[data_train['wday'] == 3].groupby('id', as_index=False)['sales', 'revenue'].mean()
    df_id_wday3.rename(columns={'sales': 'avg_sales_per_id_wday3',
                                'revenue': 'avg_revenue_per_id_wday3'}, inplace=True)
    df_id = pd.merge(df_id, df_id_wday3, on='id', how='left')
    del df_id_wday3
    gc.collect()

    df_id_wday4 = data_train[data_train['wday'] == 4].groupby('id', as_index=False)['sales', 'revenue'].mean()
    df_id_wday4.rename(columns={'sales': 'avg_sales_per_id_wday4',
                                'revenue': 'avg_revenue_per_id_wday4'}, inplace=True)
    df_id = pd.merge(df_id, df_id_wday4, on='id', how='left')
    del df_id_wday4
    gc.collect()

    df_id_wday5 = data_train[data_train['wday'] == 5].groupby('id', as_index=False)['sales', 'revenue'].mean()
    df_id_wday5.rename(columns={'sales': 'avg_sales_per_id_wday5',
                                'revenue': 'avg_revenue_per_id_wday5'}, inplace=True)
    df_id = pd.merge(df_id, df_id_wday5, on='id', how='left')
    del df_id_wday5
    gc.collect()

    df_id_wday6 = data_train[data_train['wday'] == 6].groupby('id', as_index=False)['sales', 'revenue'].mean()
    df_id_wday6.rename(columns={'sales': 'avg_sales_per_id_wday6',
                                'revenue': 'avg_revenue_per_id_wday6'}, inplace=True)
    df_id = pd.merge(df_id, df_id_wday6, on='id', how='left')
    del df_id_wday6
    gc.collect()

    df_id_wday7 = data_train[data_train['wday'] == 7].groupby('id', as_index=False)['sales', 'revenue'].mean()
    df_id_wday7.rename(columns={'sales': 'avg_sales_per_id_wday7',
                                'revenue': 'avg_revenue_per_id_wday7'}, inplace=True)
    df_id = pd.merge(df_id, df_id_wday7, on='id', how='left')
    del df_id_wday7
    gc.collect()

    # 每个月的销量和销售额均值
    df_id_month1 = data_train[data_train['month'] == 1].groupby('id', as_index=False)['sales', 'revenue'].mean()
    df_id_month1.rename(columns={'sales': 'avg_sales_per_id_month1',
                                 'revenue': 'avg_revenue_per_id_month1'}, inplace=True)
    df_id = pd.merge(df_id, df_id_month1, on='id', how='left')
    del df_id_month1
    gc.collect()

    df_id_month2 = data_train[data_train['month'] == 2].groupby('id', as_index=False)['sales', 'revenue'].mean()
    df_id_month2.rename(columns={'sales': 'avg_sales_per_id_month2',
                                 'revenue': 'avg_revenue_per_id_month2'}, inplace=True)
    df_id = pd.merge(df_id, df_id_month2, on='id', how='left')
    del df_id_month2
    gc.collect()

    df_id_month3 = data_train[data_train['month'] == 3].groupby('id', as_index=False)['sales', 'revenue'].mean()
    df_id_month3.rename(columns={'sales': 'avg_sales_per_id_month3',
                                 'revenue': 'avg_revenue_per_id_month3'}, inplace=True)
    df_id = pd.merge(df_id, df_id_month3, on='id', how='left')
    del df_id_month3
    gc.collect()

    df_id_month4 = data_train[data_train['month'] == 4].groupby('id', as_index=False)['sales', 'revenue'].mean()
    df_id_month4.rename(columns={'sales': 'avg_sales_per_id_month4',
                                 'revenue': 'avg_revenue_per_id_month4'}, inplace=True)
    df_id = pd.merge(df_id, df_id_month4, on='id', how='left')
    del df_id_month4
    gc.collect()

    df_id_month5 = data_train[data_train['month'] == 5].groupby('id', as_index=False)['sales', 'revenue'].mean()
    df_id_month5.rename(columns={'sales': 'avg_sales_per_id_month5',
                                 'revenue': 'avg_revenue_per_id_month5'}, inplace=True)
    df_id = pd.merge(df_id, df_id_month5, on='id', how='left')
    del df_id_month5
    gc.collect()

    df_id_month6 = data_train[data_train['month'] == 6].groupby('id', as_index=False)['sales', 'revenue'].mean()
    df_id_month6.rename(columns={'sales': 'avg_sales_per_id_month6',
                                 'revenue': 'avg_revenue_per_id_month6'}, inplace=True)
    df_id = pd.merge(df_id, df_id_month6, on='id', how='left')
    del df_id_month6
    gc.collect()

    df_id_month7 = data_train[data_train['month'] == 7].groupby('id', as_index=False)['sales', 'revenue'].mean()
    df_id_month7.rename(columns={'sales': 'avg_sales_per_id_month7',
                                 'revenue': 'avg_revenue_per_id_month7'}, inplace=True)
    df_id = pd.merge(df_id, df_id_month7, on='id', how='left')
    del df_id_month7
    gc.collect()

    df_id_month8 = data_train[data_train['month'] == 8].groupby('id', as_index=False)['sales', 'revenue'].mean()
    df_id_month8.rename(columns={'sales': 'avg_sales_per_id_month8',
                                 'revenue': 'avg_revenue_per_id_month8'}, inplace=True)
    df_id = pd.merge(df_id, df_id_month8, on='id', how='left')
    del df_id_month8
    gc.collect()

    df_id_month9 = data_train[data_train['month'] == 9].groupby('id', as_index=False)['sales', 'revenue'].mean()
    df_id_month9.rename(columns={'sales': 'avg_sales_per_id_month9',
                                 'revenue': 'avg_revenue_per_id_month9'}, inplace=True)
    df_id = pd.merge(df_id, df_id_month9, on='id', how='left')
    del df_id_month9
    gc.collect()

    df_id_month10 = data_train[data_train['month'] == 10].groupby('id', as_index=False)[
        'sales', 'revenue'].mean()
    df_id_month10.rename(columns={'sales': 'avg_sales_per_id_month10',
                                  'revenue': 'avg_revenue_per_id_month10'}, inplace=True)
    df_id = pd.merge(df_id, df_id_month10, on='id', how='left')
    del df_id_month10
    gc.collect()

    df_id_month11 = data_train[data_train['month'] == 11].groupby('id', as_index=False)[
        'sales', 'revenue'].mean()
    df_id_month11.rename(columns={'sales': 'avg_sales_per_id_month11',
                                  'revenue': 'avg_revenue_per_id_month11'}, inplace=True)
    df_id = pd.merge(df_id, df_id_month11, on='id', how='left')
    del df_id_month11
    gc.collect()

    df_id_month12 = data_train[data_train['month'] == 12].groupby('id', as_index=False)[
        'sales', 'revenue'].mean()
    df_id_month12.rename(columns={'sales': 'avg_sales_per_id_month12',
                                  'revenue': 'avg_revenue_per_id_month12'}, inplace=True)
    df_id = pd.merge(df_id, df_id_month12, on='id', how='left')
    del df_id_month12
    gc.collect()

    # 每个季度销售额和销量的均值
    df_id_qtr1 = data_train[data_train['quarter'] == 1].groupby('id', as_index=False)['sales', 'revenue'].mean()
    df_id_qtr1.rename(columns={'sales': 'avg_sales_per_id_qtr1',
                               'revenue': 'avg_revenue_per_id_qtr1'}, inplace=True)
    df_id = pd.merge(df_id, df_id_qtr1, on='id', how='left')
    del df_id_qtr1
    gc.collect()

    df_id_qtr2 = data_train[data_train['quarter'] == 2].groupby('id', as_index=False)['sales', 'revenue'].mean()
    df_id_qtr2.rename(columns={'sales': 'avg_sales_per_id_qtr2',
                               'revenue': 'avg_revenue_per_id_qtr2'}, inplace=True)
    df_id = pd.merge(df_id, df_id_qtr2, on='id', how='left')
    del df_id_qtr2
    gc.collect()

    df_id_qtr3 = data_train[data_train['quarter'] == 3].groupby('id', as_index=False)['sales', 'revenue'].mean()
    df_id_qtr3.rename(columns={'sales': 'avg_sales_per_id_qtr3',
                               'revenue': 'avg_revenue_per_id_qtr3'}, inplace=True)
    df_id = pd.merge(df_id, df_id_qtr3, on='id', how='left')
    del df_id_qtr3
    gc.collect()

    df_id_qtr4 = data_train[data_train['quarter'] == 4].groupby('id', as_index=False)['sales', 'revenue'].mean()
    df_id_qtr4.rename(columns={'sales': 'avg_sales_per_id_qtr4',
                               'revenue': 'avg_revenue_per_id_qtr4'}, inplace=True)
    df_id = pd.merge(df_id, df_id_qtr4, on='id', how='left')
    del df_id_qtr4
    gc.collect()

    # 在特定holiday的均值
    df_id_religious_holi_mean = data_train[data_train['is_religious_holiday'] == 1].groupby('id', as_index=False)[
        'sales', 'revenue'].mean()
    df_id_religious_holi_mean.rename(columns={'sales': 'avg_sales_per_id_on_religious_holi',
                                              'revenue': 'avg_revenue_per_id_on_religious_holi'}, inplace=True)
    df_id = pd.merge(df_id, df_id_religious_holi_mean, on='id', how='left')
    del df_id_religious_holi_mean
    gc.collect()

    df_id_national_holi_mean = data_train[data_train['is_national_holiday'] == 1].groupby('id', as_index=False)[
        'sales', 'revenue'].mean()
    df_id_national_holi_mean.rename(columns={'sales': 'avg_sales_per_id_on_national_holi',
                                             'revenue': 'avg_revenue_per_id_on_national_holi'}, inplace=True)
    df_id = pd.merge(df_id, df_id_national_holi_mean, on='id', how='left')
    del df_id_national_holi_mean
    gc.collect()

    df_id_cultural_holi_mean = data_train[data_train['is_cultural_holiday'] == 1].groupby('id', as_index=False)[
        'sales', 'revenue'].mean()
    df_id_cultural_holi_mean.rename(columns={'sales': 'avg_sales_per_id_on_cultural_holi',
                                             'revenue': 'avg_revenue_per_id_on_cultural_holi'}, inplace=True)
    df_id = pd.merge(df_id, df_id_cultural_holi_mean, on='id', how='left')
    del df_id_cultural_holi_mean
    gc.collect()

    df_id_sporting_holi_mean = data_train[data_train['is_sporting_holiday'] == 1].groupby('id', as_index=False)[
        'sales', 'revenue'].mean()
    df_id_sporting_holi_mean.rename(columns={'sales': 'avg_sales_per_id_on_sporting_holi',
                                             'revenue': 'avg_revenue_per_id_on_sporting_holi'}, inplace=True)
    df_id = pd.merge(df_id, df_id_sporting_holi_mean, on='id', how='left')
    del df_id_sporting_holi_mean
    gc.collect()

    ##——————————————————————————————————————————
    # snap对每一个id的影响
    df_id_snap_mean = data_train.groupby(['id', 'snap'], as_index=False)['revenue', 'sales'].mean()
    df_id_snap_mean.rename(columns={'sales': 'avg_sales_per_id_snap', 'revenue': 'avg_revenue_per_id_snap'},
                           inplace=True)

    # 是否周末对每一个id的影响
    df_id_weekend_mean = data_train.groupby(['id', 'is_weekend'], as_index=False)['revenue', 'sales'].mean()
    df_id_weekend_mean.rename(
        columns={'sales': 'avg_sales_per_id_weekend', 'revenue': 'avg_revenue_per_id_weekend'}, inplace=True)

    # 是否季末对每一个id的影响
    df_id_qtr_end_mean = data_train.groupby(['id', 'is_quarter_end'], as_index=False)['revenue', 'sales'].mean()
    df_id_qtr_end_mean.rename(
        columns={'sales': 'avg_sales_per_id_qtr_end', 'revenue': 'avg_revenue_per_id_qtr_end'}, inplace=True)

    return df_id, df_id_snap_mean, df_id_weekend_mean, df_id_qtr_end_mean
