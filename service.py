import requests
import pandas as pd
import numpy as np


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
