import requests
import pandas as pd
from pymongo import MongoClient

client = MongoClient('mongodb://zeroPass:9674Ephzx&T7@127.0.0.1:27017/?retryWrites=true&w=majority')

def financial_statement(year, season, com_code):
    if year >= 1000:
        year -= 1911

    url = "https://mops.twse.com.tw/mops/web/ajax_t164sb04"

    r = requests.post(url, {
        'encodeURIComponent': 1,
        'step': 2,
        'firstin': 1,
        'TYPEK': 'pub',
        'year': str(year),
        'season': str(season),
        'co_id': com_code
    })

    r.encoding = 'utf8'
    dfs = pd.read_html(r.text, header=None)
    print(dfs)
    # return pd.concat(dfs[1:], axis=0, sort=False)\
    #          .set_index(['公司代號'])\
    #          .apply(lambda s: pd.to_numeric(s, errors='ceorce'))


def read_form_html(path: str):
    dfs = pd.read_html(path, header=None)
    merged_df = pd.merge(dfs[1], dfs[2], on=['跨類別查詢', '跨類別查詢.1'], how='inner')
    # rename columns name
    merged_df = merged_df.rename(columns={'跨類別查詢': '時間', '跨類別查詢.1': '銀行'})
    return merged_df


def insert_mongo_by_time(merged_df):
    db = client['time_series']

    for index, row in merged_df.iterrows():
        row_dict = row.to_dict()
        # store collection by 時間
        temp = row_dict['時間'].split(' ')
        collection = db[temp[0]]
        if len(temp) > 1:
            if temp[1] in ['3月', '6月', '9月', '12月']:
                # insert data
                collection.insert_one(row_dict)

def insert_mongo_by_bank(merge_df):
    db = client['bank']
    store_list = []
    for index, row in merge_df.iterrows():
        # init key by 銀行 and value is list
        if row['銀行'] not in store_list:
            store_list.append(row['銀行'])
        # store collection by 銀行
        collection = db[row['銀行']]
        # insert data
        temp = row['時間'].split(' ')
        if len(temp) > 1:
            if temp[1] in ['3月', '6月', '9月', '12月']:
                # insert data
                collection.insert_one(row.to_dict())