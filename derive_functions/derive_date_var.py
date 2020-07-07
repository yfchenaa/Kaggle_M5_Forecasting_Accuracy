import pandas as pd
import numpy as np
import random
from datetime import timedelta
from dateutil.relativedelta import relativedelta
import datetime as dt
import gc


def derive_calender_feats(df_calender, df_calender_extended):
    data_calender = df_calender.copy(deep=True)
    data_calender_extended = df_calender_extended.copy(deep=True)
    del df_calender, df_calender_extended
    gc.collect()
    # 生成日期与节假日相关变量

    data_calender_full = data_calender.append(data_calender_extended).reset_index(drop=True)
    data_calender_full = data_calender_full[data_calender.columns]

    del data_calender, data_calender_extended
    gc.collect()

    # 填充黑色星期五信息
    data_calender_full.loc[data_calender_full['date'].isin(
        ['2014-11-28', '2015-11-27', '2016-11-25', '2013-11-29']), 'event_type_1'] = 'National'
    data_calender_full.loc[data_calender_full['date'].isin(
        ['2014-11-28', '2015-11-27', '2016-11-25', '2013-11-29']), 'event_name_1'] = 'BlackFriday'

    # 生成本周，上周，下周，本月，上月，下月的数据
    data_calender_full['ThisWeek'] = data_calender_full.apply(
        lambda x: dt.datetime.strptime(x['date'], '%Y-%m-%d').strftime('%Y-%W'), axis=1)
    data_calender_full['LastWeek'] = data_calender_full.apply(
        lambda x: (dt.datetime.strptime(x['date'], '%Y-%m-%d') - timedelta(weeks=1)).strftime('%Y-%W'), axis=1)
    data_calender_full['NextWeek'] = data_calender_full.apply(
        lambda x: (dt.datetime.strptime(x['date'], '%Y-%m-%d') + timedelta(weeks=1)).strftime('%Y-%W'), axis=1)

    data_calender_full['ThisMonth'] = data_calender_full.apply(
        lambda x: dt.datetime.strptime(x['date'], '%Y-%m-%d').strftime('%Y-%m'), axis=1)
    data_calender_full['LastMonth'] = data_calender_full.apply(
        lambda x: (dt.datetime.strptime(x['date'], '%Y-%m-%d') - relativedelta(months=1)).strftime('%Y-%m'), axis=1)
    data_calender_full['NextMonth'] = data_calender_full.apply(
        lambda x: (dt.datetime.strptime(x['date'], '%Y-%m-%d') + relativedelta(months=1)).strftime('%Y-%m'), axis=1)

    # 生成是否为节假日的虚拟变量
    data_calender_full['is_religious_holiday'] = np.where((data_calender_full['event_type_1'] == 'Religious') |
                                                          (data_calender_full['event_type_2'] == 'Religious'), 1, 0)
    data_calender_full['is_national_holiday'] = np.where((data_calender_full['event_type_1'] == 'National') |
                                                         (data_calender_full['event_type_2'] == 'National'), 1, 0)
    data_calender_full['is_cultural_holiday'] = np.where((data_calender_full['event_type_1'] == 'Cultural') |
                                                         (data_calender_full['event_type_2'] == 'Cultural'), 1, 0)
    data_calender_full['is_sporting_holiday'] = np.where((data_calender_full['event_type_1'] == 'Sporting') |
                                                         (data_calender_full['event_type_2'] == 'Sporting'), 1, 0)
    data_calender_full['num_holidays'] = data_calender_full[['is_religious_holiday', 'is_national_holiday',
                                                             'is_cultural_holiday', 'is_sporting_holiday']].sum(axis=1)

    # 获取每周和每月各个holiday的数量
    data_ReligiousHoliday_month = (data_calender_full[['date', 'ThisMonth', 'is_religious_holiday']]
                                   .drop_duplicates(subset=['date'], keep='last')).groupby('ThisMonth')[
        'is_religious_holiday'].sum()
    data_ReligiousHoliday_week = (data_calender_full[['date', 'ThisWeek', 'is_religious_holiday']]
                                  .drop_duplicates(subset=['date'], keep='last')).groupby('ThisWeek')[
        'is_religious_holiday'].sum()

    data_NationalHoliday_month = (data_calender_full[['date', 'ThisMonth', 'is_national_holiday']]
                                  .drop_duplicates(subset=['date'], keep='last')).groupby('ThisMonth')[
        'is_national_holiday'].sum()
    data_NationalHoliday_week = (data_calender_full[['date', 'ThisWeek', 'is_national_holiday']]
                                 .drop_duplicates(subset=['date'], keep='last')).groupby('ThisWeek')[
        'is_national_holiday'].sum()

    data_CulturalHoliday_month = (data_calender_full[['date', 'ThisMonth', 'is_cultural_holiday']]
                                  .drop_duplicates(subset=['date'], keep='last')).groupby('ThisMonth')[
        'is_cultural_holiday'].sum()
    data_CulturalHoliday_week = (data_calender_full[['date', 'ThisWeek', 'is_cultural_holiday']]
                                 .drop_duplicates(subset=['date'], keep='last')).groupby('ThisWeek')[
        'is_cultural_holiday'].sum()

    data_SportingHoliday_month = (data_calender_full[['date', 'ThisMonth', 'is_sporting_holiday']]
                                  .drop_duplicates(subset=['date'], keep='last')).groupby('ThisMonth')[
        'is_sporting_holiday'].sum()
    data_SportingHoliday_week = (data_calender_full[['date', 'ThisWeek', 'is_sporting_holiday']]
                                 .drop_duplicates(subset=['date'], keep='last')).groupby('ThisWeek')[
        'is_sporting_holiday'].sum()

    data_AllHoliday_month = (data_calender_full[['date', 'ThisMonth', 'num_holidays']]
                             .drop_duplicates(subset=['date'], keep='last')).groupby('ThisMonth')['num_holidays'].sum()
    data_AllHoliday_week = (data_calender_full[['date', 'ThisWeek', 'num_holidays']]
                            .drop_duplicates(subset=['date'], keep='last')).groupby('ThisWeek')['num_holidays'].sum()

    data_calender_full['ReligiousHolidayNextMonth'] = data_calender_full['NextMonth'].map(
        dict(data_ReligiousHoliday_month))
    data_calender_full['ReligiousHolidayLastMonth'] = data_calender_full['LastMonth'].map(
        dict(data_ReligiousHoliday_month))
    data_calender_full['ReligiousHolidayThisMonth'] = data_calender_full['ThisMonth'].map(
        dict(data_ReligiousHoliday_month))

    data_calender_full['ReligiousHolidayNextWeek'] = data_calender_full['NextWeek'].map(
        dict(data_ReligiousHoliday_week))
    data_calender_full['ReligiousHolidayLastWeek'] = data_calender_full['LastWeek'].map(
        dict(data_ReligiousHoliday_week))
    data_calender_full['ReligiousHolidayThisWeek'] = data_calender_full['ThisWeek'].map(
        dict(data_ReligiousHoliday_week))

    data_calender_full['NationalHolidayNextMonth'] = data_calender_full['NextMonth'].map(
        dict(data_NationalHoliday_month))
    data_calender_full['NationalHolidayLastMonth'] = data_calender_full['LastMonth'].map(
        dict(data_NationalHoliday_month))
    data_calender_full['NationalHolidayThisMonth'] = data_calender_full['ThisMonth'].map(
        dict(data_NationalHoliday_month))

    data_calender_full['NationalHolidayNextWeek'] = data_calender_full['NextWeek'].map(dict(data_NationalHoliday_week))
    data_calender_full['NationalHolidayLastWeek'] = data_calender_full['LastWeek'].map(dict(data_NationalHoliday_week))
    data_calender_full['NationalHolidayThisWeek'] = data_calender_full['ThisWeek'].map(dict(data_NationalHoliday_week))

    data_calender_full['CulturalHolidayNextMonth'] = data_calender_full['NextMonth'].map(
        dict(data_CulturalHoliday_month))
    data_calender_full['CulturalHolidayLastMonth'] = data_calender_full['LastMonth'].map(
        dict(data_CulturalHoliday_month))
    data_calender_full['CulturalHolidayThisMonth'] = data_calender_full['ThisMonth'].map(
        dict(data_CulturalHoliday_month))

    data_calender_full['CulturalHolidayNextWeek'] = data_calender_full['NextWeek'].map(dict(data_CulturalHoliday_week))
    data_calender_full['CulturalHolidayLastWeek'] = data_calender_full['LastWeek'].map(dict(data_CulturalHoliday_week))
    data_calender_full['CulturalHolidayThisWeek'] = data_calender_full['ThisWeek'].map(dict(data_CulturalHoliday_week))

    data_calender_full['SportingHolidayNextMonth'] = data_calender_full['NextMonth'].map(
        dict(data_SportingHoliday_month))
    data_calender_full['SportingHolidayLastMonth'] = data_calender_full['LastMonth'].map(
        dict(data_SportingHoliday_month))
    data_calender_full['SportingHolidayThisMonth'] = data_calender_full['ThisMonth'].map(
        dict(data_SportingHoliday_month))

    data_calender_full['SportingHolidayNextWeek'] = data_calender_full['NextWeek'].map(dict(data_SportingHoliday_week))
    data_calender_full['SportingHolidayLastWeek'] = data_calender_full['LastWeek'].map(dict(data_SportingHoliday_week))
    data_calender_full['SportingHolidayThisWeek'] = data_calender_full['ThisWeek'].map(dict(data_SportingHoliday_week))

    data_calender_full['AllHolidayNextMonth'] = data_calender_full['NextMonth'].map(dict(data_AllHoliday_month))
    data_calender_full['AllHolidayLastMonth'] = data_calender_full['LastMonth'].map(dict(data_AllHoliday_month))
    data_calender_full['AllHolidayThisMonth'] = data_calender_full['ThisMonth'].map(dict(data_AllHoliday_month))

    data_calender_full['AllHolidayNextWeek'] = data_calender_full['NextWeek'].map(dict(data_AllHoliday_week))
    data_calender_full['AllHolidayLastWeek'] = data_calender_full['LastWeek'].map(dict(data_AllHoliday_week))
    data_calender_full['AllHolidayThisWeek'] = data_calender_full['ThisWeek'].map(dict(data_AllHoliday_week))

    data_calender_full = data_calender_full.iloc[:1969]
    for i in data_calender_full.columns[-30:]:
        data_calender_full[i] = data_calender_full[i].fillna(0)

    return data_calender_full
