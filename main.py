from service import financial_statement, read_form_html, insert_mongo_by_time, insert_mongo_by_bank

if __name__ == '__main__':
    # print(financial_statement(2014, 2, 5843))
    merge_df = read_form_html("./data/data_1.html")
    # insert_mongo_by_time(merge_df)
    # insert_mongo_by_bank(merge_df)
