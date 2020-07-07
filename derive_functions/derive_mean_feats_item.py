import pandas as pd
import gc
import warnings

warnings.filterwarnings('ignore')


def get_df_item(df_train):
    use_cols = ['wday', 'item_id', 'date', 'sales', 'revenue', 'month', 'quarter', 'is_religious_holiday',
                'is_national_holiday', 'is_cultural_holiday', 'is_sporting_holiday', 'snap', 'sell_price',
                'is_weekend', 'is_quarter_end']
    data_train = df_train[use_cols].copy(deep=True)
    del df_train
    gc.collect()

    # 每个item不加条件的均值
    df_item_mean = data_train.groupby('item_id', as_index=False)['sales', 'revenue'].mean()
    df_item_mean.rename(columns={'sales': 'avg_sales_per_item', 'revenue': 'avg_revenue_per_item'}, inplace=True)

    # 每周每天的销量和销售额均值
    df_item_wday1 = data_train[data_train['wday'] == 1].groupby('item_id', as_index=False)['sales', 'revenue'].mean()
    df_item_wday1.rename(columns={'sales': 'avg_sales_per_item_wday1',
                                  'revenue': 'avg_revenue_per_item_wday1'}, inplace=True)
    df_item = pd.merge(df_item_mean, df_item_wday1, on='item_id', how='left')
    del df_item_wday1
    gc.collect()

    df_item_wday2 = data_train[data_train['wday'] == 2].groupby('item_id', as_index=False)['sales', 'revenue'].mean()
    df_item_wday2.rename(columns={'sales': 'avg_sales_per_item_wday2',
                                  'revenue': 'avg_revenue_per_item_wday2'}, inplace=True)
    df_item = pd.merge(df_item, df_item_wday2, on='item_id', how='left')
    del df_item_wday2
    gc.collect()

    df_item_wday3 = data_train[data_train['wday'] == 3].groupby('item_id', as_index=False)['sales', 'revenue'].mean()
    df_item_wday3.rename(columns={'sales': 'avg_sales_per_item_wday3',
                                  'revenue': 'avg_revenue_per_item_wday3'}, inplace=True)
    df_item = pd.merge(df_item, df_item_wday3, on='item_id', how='left')
    del df_item_wday3
    gc.collect()

    df_item_wday4 = data_train[data_train['wday'] == 4].groupby('item_id', as_index=False)['sales', 'revenue'].mean()
    df_item_wday4.rename(columns={'sales': 'avg_sales_per_item_wday4',
                                  'revenue': 'avg_revenue_per_item_wday4'}, inplace=True)
    df_item = pd.merge(df_item, df_item_wday4, on='item_id', how='left')
    del df_item_wday4
    gc.collect()

    df_item_wday5 = data_train[data_train['wday'] == 5].groupby('item_id', as_index=False)['sales', 'revenue'].mean()
    df_item_wday5.rename(columns={'sales': 'avg_sales_per_item_wday5',
                                  'revenue': 'avg_revenue_per_item_wday5'}, inplace=True)
    df_item = pd.merge(df_item, df_item_wday5, on='item_id', how='left')
    del df_item_wday5
    gc.collect()

    df_item_wday6 = data_train[data_train['wday'] == 6].groupby('item_id', as_index=False)['sales', 'revenue'].mean()
    df_item_wday6.rename(columns={'sales': 'avg_sales_per_item_wday6',
                                  'revenue': 'avg_revenue_per_item_wday6'}, inplace=True)
    df_item = pd.merge(df_item, df_item_wday6, on='item_id', how='left')
    del df_item_wday6
    gc.collect()

    df_item_wday7 = data_train[data_train['wday'] == 7].groupby('item_id', as_index=False)['sales', 'revenue'].mean()
    df_item_wday7.rename(columns={'sales': 'avg_sales_per_item_wday7',
                                  'revenue': 'avg_revenue_per_item_wday7'}, inplace=True)
    df_item = pd.merge(df_item, df_item_wday7, on='item_id', how='left')
    del df_item_wday7
    gc.collect()

    # 每个月的销量和销售额均值
    df_item_month1 = data_train[data_train['month'] == 1].groupby('item_id', as_index=False)['sales', 'revenue'].mean()
    df_item_month1.rename(columns={'sales': 'avg_sales_per_item_month1',
                                   'revenue': 'avg_revenue_per_item_month1'}, inplace=True)
    df_item = pd.merge(df_item, df_item_month1, on='item_id', how='left')
    del df_item_month1
    gc.collect()

    df_item_month2 = data_train[data_train['month'] == 2].groupby('item_id', as_index=False)['sales', 'revenue'].mean()
    df_item_month2.rename(columns={'sales': 'avg_sales_per_item_month2',
                                   'revenue': 'avg_revenue_per_item_month2'}, inplace=True)
    df_item = pd.merge(df_item, df_item_month2, on='item_id', how='left')
    del df_item_month2
    gc.collect()

    df_item_month3 = data_train[data_train['month'] == 3].groupby('item_id', as_index=False)['sales', 'revenue'].mean()
    df_item_month3.rename(columns={'sales': 'avg_sales_per_item_month3',
                                   'revenue': 'avg_revenue_per_item_month3'}, inplace=True)
    df_item = pd.merge(df_item, df_item_month3, on='item_id', how='left')
    del df_item_month3
    gc.collect()

    df_item_month4 = data_train[data_train['month'] == 4].groupby('item_id', as_index=False)['sales', 'revenue'].mean()
    df_item_month4.rename(columns={'sales': 'avg_sales_per_item_month4',
                                   'revenue': 'avg_revenue_per_item_month4'}, inplace=True)
    df_item = pd.merge(df_item, df_item_month4, on='item_id', how='left')
    del df_item_month4
    gc.collect()

    df_item_month5 = data_train[data_train['month'] == 5].groupby('item_id', as_index=False)['sales', 'revenue'].mean()
    df_item_month5.rename(columns={'sales': 'avg_sales_per_item_month5',
                                   'revenue': 'avg_revenue_per_item_month5'}, inplace=True)
    df_item = pd.merge(df_item, df_item_month5, on='item_id', how='left')
    del df_item_month5
    gc.collect()

    df_item_month6 = data_train[data_train['month'] == 6].groupby('item_id', as_index=False)['sales', 'revenue'].mean()
    df_item_month6.rename(columns={'sales': 'avg_sales_per_item_month6',
                                   'revenue': 'avg_revenue_per_item_month6'}, inplace=True)
    df_item = pd.merge(df_item, df_item_month6, on='item_id', how='left')
    del df_item_month6
    gc.collect()

    df_item_month7 = data_train[data_train['month'] == 7].groupby('item_id', as_index=False)['sales', 'revenue'].mean()
    df_item_month7.rename(columns={'sales': 'avg_sales_per_item_month7',
                                   'revenue': 'avg_revenue_per_item_month7'}, inplace=True)
    df_item = pd.merge(df_item, df_item_month7, on='item_id', how='left')
    del df_item_month7
    gc.collect()

    df_item_month8 = data_train[data_train['month'] == 8].groupby('item_id', as_index=False)['sales', 'revenue'].mean()
    df_item_month8.rename(columns={'sales': 'avg_sales_per_item_month8',
                                   'revenue': 'avg_revenue_per_item_month8'}, inplace=True)
    df_item = pd.merge(df_item, df_item_month8, on='item_id', how='left')
    del df_item_month8
    gc.collect()

    df_item_month9 = data_train[data_train['month'] == 9].groupby('item_id', as_index=False)['sales', 'revenue'].mean()
    df_item_month9.rename(columns={'sales': 'avg_sales_per_item_month9',
                                   'revenue': 'avg_revenue_per_item_month9'}, inplace=True)
    df_item = pd.merge(df_item, df_item_month9, on='item_id', how='left')
    del df_item_month9
    gc.collect()

    df_item_month10 = data_train[data_train['month'] == 10].groupby('item_id', as_index=False)[
        'sales', 'revenue'].mean()
    df_item_month10.rename(columns={'sales': 'avg_sales_per_item_month10',
                                    'revenue': 'avg_revenue_per_item_month10'}, inplace=True)
    df_item = pd.merge(df_item, df_item_month10, on='item_id', how='left')
    del df_item_month10
    gc.collect()

    df_item_month11 = data_train[data_train['month'] == 11].groupby('item_id', as_index=False)[
        'sales', 'revenue'].mean()
    df_item_month11.rename(columns={'sales': 'avg_sales_per_item_month11',
                                    'revenue': 'avg_revenue_per_item_month11'}, inplace=True)
    df_item = pd.merge(df_item, df_item_month11, on='item_id', how='left')
    del df_item_month11
    gc.collect()

    df_item_month12 = data_train[data_train['month'] == 12].groupby('item_id', as_index=False)[
        'sales', 'revenue'].mean()
    df_item_month12.rename(columns={'sales': 'avg_sales_per_item_month12',
                                    'revenue': 'avg_revenue_per_item_month12'}, inplace=True)
    df_item = pd.merge(df_item, df_item_month12, on='item_id', how='left')
    del df_item_month12
    gc.collect()

    # 每个季度销售额和销量的均值
    df_item_qtr1 = data_train[data_train['quarter'] == 1].groupby('item_id', as_index=False)['sales', 'revenue'].mean()
    df_item_qtr1.rename(columns={'sales': 'avg_sales_per_item_qtr1',
                                 'revenue': 'avg_revenue_per_item_qtr1'}, inplace=True)
    df_item = pd.merge(df_item, df_item_qtr1, on='item_id', how='left')
    del df_item_qtr1
    gc.collect()

    df_item_qtr2 = data_train[data_train['quarter'] == 2].groupby('item_id', as_index=False)['sales', 'revenue'].mean()
    df_item_qtr2.rename(columns={'sales': 'avg_sales_per_item_qtr2',
                                 'revenue': 'avg_revenue_per_item_qtr2'}, inplace=True)
    df_item = pd.merge(df_item, df_item_qtr2, on='item_id', how='left')
    del df_item_qtr2
    gc.collect()

    df_item_qtr3 = data_train[data_train['quarter'] == 3].groupby('item_id', as_index=False)['sales', 'revenue'].mean()
    df_item_qtr3.rename(columns={'sales': 'avg_sales_per_item_qtr3',
                                 'revenue': 'avg_revenue_per_item_qtr3'}, inplace=True)
    df_item = pd.merge(df_item, df_item_qtr3, on='item_id', how='left')
    del df_item_qtr3
    gc.collect()

    df_item_qtr4 = data_train[data_train['quarter'] == 4].groupby('item_id', as_index=False)['sales', 'revenue'].mean()
    df_item_qtr4.rename(columns={'sales': 'avg_sales_per_item_qtr4',
                                 'revenue': 'avg_revenue_per_item_qtr4'}, inplace=True)
    df_item = pd.merge(df_item, df_item_qtr4, on='item_id', how='left')
    del df_item_qtr4
    gc.collect()

    # 在特定holiday的均值
    df_item_religious_holi_mean = \
        data_train[data_train['is_religious_holiday'] == 1].groupby('item_id', as_index=False)[
            'sales', 'revenue'].mean()
    df_item_religious_holi_mean.rename(columns={'sales': 'avg_sales_per_item_on_religious_holi',
                                                'revenue': 'avg_revenue_per_item_on_religious_holi'}, inplace=True)
    df_item = pd.merge(df_item, df_item_religious_holi_mean, on='item_id', how='left')
    del df_item_religious_holi_mean
    gc.collect()

    df_item_national_holi_mean = data_train[data_train['is_national_holiday'] == 1].groupby('item_id', as_index=False)[
        'sales', 'revenue'].mean()
    df_item_national_holi_mean.rename(columns={'sales': 'avg_sales_per_item_on_national_holi',
                                               'revenue': 'avg_revenue_per_item_on_national_holi'}, inplace=True)
    df_item = pd.merge(df_item, df_item_national_holi_mean, on='item_id', how='left')
    del df_item_national_holi_mean
    gc.collect()

    df_item_cultural_holi_mean = data_train[data_train['is_cultural_holiday'] == 1].groupby('item_id', as_index=False)[
        'sales', 'revenue'].mean()
    df_item_cultural_holi_mean.rename(columns={'sales': 'avg_sales_per_item_on_cultural_holi',
                                               'revenue': 'avg_revenue_per_item_on_cultural_holi'}, inplace=True)
    df_item = pd.merge(df_item, df_item_cultural_holi_mean, on='item_id', how='left')
    del df_item_cultural_holi_mean
    gc.collect()

    df_item_sporting_holi_mean = data_train[data_train['is_sporting_holiday'] == 1].groupby('item_id', as_index=False)[
        'sales', 'revenue'].mean()
    df_item_sporting_holi_mean.rename(columns={'sales': 'avg_sales_per_item_on_sporting_holi',
                                               'revenue': 'avg_revenue_per_item_on_sporting_holi'}, inplace=True)
    df_item = pd.merge(df_item, df_item_sporting_holi_mean, on='item_id', how='left')
    del df_item_sporting_holi_mean
    gc.collect()

    ##——————————————————————————————————————————
    # snap对每一个item_id的影响
    df_item_snap_mean = data_train.groupby(['item_id', 'snap'], as_index=False)['revenue', 'sales'].mean()
    df_item_snap_mean.rename(columns={'sales': 'avg_sales_per_item_snap', 'revenue': 'avg_revenue_per_item_snap'},
                             inplace=True)

    # 是否周末对每一个item_id的影响
    df_item_weekend_mean = data_train.groupby(['item_id', 'is_weekend'], as_index=False)['revenue', 'sales'].mean()
    df_item_weekend_mean.rename(
        columns={'sales': 'avg_sales_per_item_weekend', 'revenue': 'avg_revenue_per_item_weekend'}, inplace=True)

    # 是否季末对每一个item_id的影响
    df_item_qtr_end_mean = data_train.groupby(['item_id', 'is_quarter_end'], as_index=False)['revenue', 'sales'].mean()
    df_item_qtr_end_mean.rename(
        columns={'sales': 'avg_sales_per_item_qtr_end', 'revenue': 'avg_revenue_per_item_qtr_end'}, inplace=True)
    ##——————————————————————————————————————————
    # 每个item每天的大众价格
    df_item_price_mode = data_train[['item_id', 'date', 'sell_price']].groupby(['item_id', 'date'], as_index=False).agg(
        lambda x: x.value_counts().index[0]).reset_index()
    df_item_price_mode.rename(columns={'sell_price': 'item_sell_price_mode'}, inplace=True)
    return df_item, df_item_snap_mean, df_item_weekend_mean, df_item_qtr_end_mean, df_item_price_mode
