import requests
import pandas as pd
from pymongo import MongoClient

client = MongoClient('mongodb://zeroPass:9674Ephzx&T7@127.0.0.1:27017/?retryWrites=true&w=majority')
bank_db = client['bank']
time_db = client['time_series']

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
    for index, row in merged_df.iterrows():
        row_dict = row.to_dict()
        # store collection by 時間
        temp = row_dict['時間'].split(' ')
        collection = time_db[temp[0]]
        if len(temp) > 1:
            if temp[1] in ['3月', '6月', '9月', '12月']:
                if " " in row_dict.get('銀行'):
                    row_dict['銀行'] = row_dict['銀行'].split(" ")[1]
                if "＊" in row_dict.get('銀行'):
                    row_dict['銀行'] = row_dict['銀行'].replace("＊", "")
                # insert data
                collection.insert_one(row_dict)


def insert_mongo_by_bank(merge_df):
    store_list = []
    for index, row in merge_df.iterrows():
        # init key by 銀行 and value is list
        if row['銀行'] not in store_list:
            store_list.append(row['銀行'])
        row_dict = row.to_dict()
        # store collection by 銀行
        bank = row_dict['銀行']
        if " " in bank:
            bank = bank.split(" ")[1]
        if "＊" in bank:
            bank = bank.replace("＊", "")
        collection = bank_db[bank]
        # insert data
        temp = row['時間'].split(' ')

        if len(temp) > 1:
            if temp[1] in ['3月', '6月', '9月', '12月']:
                if " " in row_dict.get('銀行'):
                    row_dict['銀行'] = row_dict['銀行'].split(" ")[1]
                if "＊" in row_dict.get('銀行'):
                    row_dict['銀行'] = row_dict['銀行'].replace("＊", "")
                # insert data
                collection.insert_one(row_dict)


def download_bstatistics_view():
    for i in range(105, 112):
        # range 3, ,6 ,9 ,12
        for j in range(3, 13, 3):
            download_xls(
                'https://www.banking.gov.tw/webdowndoc?file=/stat/lc/12081400({}).xls'.format(str(i) + str(j).zfill(2)),
                str(i) + str(j).zfill(2))


# download xls from url to data folder
def download_xls(url, file_name):
    r = requests.get(url)
    with open('./data/{}.xls'.format(file_name), 'wb') as f:
        f.write(r.content)


# process xls
def process_xls(file_name):
    print("file_name: ", file_name)
    query_time = get_time(file_name)
    collection = time_db[query_time[:4]]

    xls = pd.ExcelFile('./data/{}.xls'.format(file_name))
    sheet_names = xls.sheet_names
    result = {}
    # 讀取第一個工作表並創建一個新的DataFrame
    merged_df = pd.read_excel(xls, sheet_name=sheet_names[0])
    local_back = parser_first_sheet(merged_df, collection, query_time)
    result['進出口信用狀金額統計'] = local_back

    column_list = ['開發進口信用狀統計', '通知出口信用狀統計', '辦理出口信用狀貸款統計', '辦理出口信用狀押匯統計']
    # result set column_list to key and value is list
    for column in column_list:
        result[column] = []
    count = 0
    # 將其他工作表追加到剛剛建立的DataFrame
    for sheet_name in sheet_names[1:5]:
        df = pd.read_excel(xls, sheet_name=sheet_name)
        for index, row in df.iterrows():
            row_dict = row.to_dict()
            # check row_dict.get("公\u3000開\u3000類") is str
            if isinstance(row_dict.get("公\u3000開\u3000類"), str) and "資料來源" not in row_dict.get(
                    "公\u3000開\u3000類") and '銀行' in row_dict.get("公\u3000開\u3000類"):
                # 指定查詢條件
                if row_dict.get("公\u3000開\u3000類") == "台北富邦銀行":
                    query = {"時間": query_time, "銀行": "台北富邦商業銀行"}
                else:
                    query = {"時間": query_time, "銀行": row_dict.get("公\u3000開\u3000類")}
                update_record(collection, query, {column_list[count]: row_dict['Unnamed: 2']})
                result[column_list[count]].append({row_dict.get("公\u3000開\u3000類"): row_dict['Unnamed: 2']})
        count += 1


def parser_first_sheet(df, collection, query_time):
    temp_list = []

    for index, row in df.iterrows():
        row_dict = row.to_dict()
        if row_dict['Unnamed: 2'] == '本 國 銀 行':
            temp_list.append(str(row_dict['Unnamed: 3']))

    return_data = {
        "開發進口信用狀": int(temp_list[0].replace(",", "")),
        "通知出口信用狀": int(temp_list[1].replace(",", "")),
        "辦理出口信用狀貸款": int(temp_list[2].replace(",", "")),
        "辦理出口信用狀押匯": int(temp_list[3].replace(",", "")),
    }

    # 指定查詢條件
    query = {"時間": query_time, "銀行": "本國銀行"}
    update_record(collection, query, return_data)

    return return_data


def get_time(file_name):
    query_time = "{}年 {}月".format(file_name[:3], file_name[3:])
    if query_time[5] == '0':
        # remove 0
        query_time = query_time[:5] + query_time[6:]
    return query_time


def update_record(collection, query, new_key_value):
    # 在查詢到的文檔中添加新的鍵值對
    collection.update_one(query, {"$set": new_key_value})
    bank = bank_db[query['銀行']]
    bank.update_one(query, {"$set": new_key_value})

# export all data to csv
def export_csv():
    export_csv_by_db(time_db, True)
    export_csv_by_db(bank_db, False)

def export_csv_by_db(db, merge):
    collection_names = db.list_collection_names()

    all_data_df = pd.DataFrame()
    for collection_name in collection_names:
        # 讀取集合中的所有文檔
        documents = db[collection_name].find()

        # 使用pandas將文檔轉換為DataFrame
        df = pd.DataFrame(list(documents))

        # 刪除_id列
        if '_id' in df.columns:
            df.drop('_id', axis=1, inplace=True)

        if merge:
            all_data_df = pd.concat([all_data_df, df], ignore_index=True)

        # 將DataFrame保存為CSV文件
        csv_file_name = f"./export/{collection_name}.csv"
        df.to_csv(csv_file_name, index=False)
        print(f"Exported {csv_file_name}")

    if merge:
        all_data_df.to_csv("./export/all_data.csv", index=False)
        print("Exported all_data.csv")