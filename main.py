from service import financial_statement, read_form_html, insert_mongo_by_time, insert_mongo_by_bank, download_xls, \
    download_bstatistics_view, process_xls, export_csv, parser_bis, read_from_data_html, read_from_e_data, \
    export_time_data

if __name__ == '__main__':
    # parser html from 衍生性金融商品 html and save to mongo
    # merge_df = read_form_html(["./data/data_1.html", "./data/data_2.html"])
    # insert_mongo_by_time(merge_df)
    # insert_mongo_by_bank(merge_df)

    # download all quarter xls from 進出口信用狀金額統計
    # download_bstatistics_view()

    # parser all xls to update data
    # for i in range(105, 112):
    #     for j in range(3, 13, 3):
    #         if j < 10:
    #             process_xls(str(i) + "0" + str(j))
    #         else:
    #             process_xls(str(i) + str(j))

    # parser data/data html
    read_from_data_html()

    # parser bis xls to update data
    parser_bis()

    # parser e_data
    read_from_e_data()

    # export all data to csv
    # export_csv()

    # export all data in one
    export_time_data()
