import os
import re
import datetime as dt
import numpy as np
import pandas as pd

def kanji_to_stadium_code(kanji):
    dict_stadium = {'桐生': 'KRY', '戸田': 'TDA', '江戸川': 'EDG', '平和島': 'HWJ',
                    '多摩川': 'TMG', '浜名湖': 'HMN', '蒲郡': 'GMG', '常滑': 'TKN',
                    '津': 'TSU', '三国': 'MKN', '琵琶湖': 'BWK', '住之江': 'SME',
                    '尼崎': 'AMG', '鳴門': 'NRT', '丸亀': 'MRG', '児島': 'KJM',
                    '宮島': 'MYJ', '徳山': 'TKY', '下関': 'SMS', '若松': 'WKM',
                    '芦屋': 'ASY', '福岡': 'FKO', '唐津': 'KRT', '大村': 'OMR', 'びわこ' : 'BWK'
                    }
    retval = ''
    try:
        retval = dict_stadium[kanji]
    finally:
        pass
    
    if retval == '':
        print('kanji_to_statium_code関数での変換に失敗')
        
    return retval
    

def create_race_code(date, stadium_code, race_round):
    # レースコードを生成
    stadium_code = kanji_to_stadium_code(stadium_code)
    return date.strftime('%Y%m%d') + stadium_code + race_round

def dict_stadium(code):
    dict_stadium = {'KRY': '01', 'TDA': '02', 'EDG': '03', 'HWJ': '04',
                    'TMG': '05', 'HMN': '06', 'GMG': '07', 'TKN': '08',
                    'TSU': '09', 'MKN': '10', 'BWK': '11', 'SME': '12',
                    'AMG': '13', 'NRT': '14', 'MRG': '15', 'KJM': '16',
                    'MYJ': '17', 'TKY': '18', 'SMS': '19', 'WKM': '20',
                    'ASY': '21', 'FKO': '22', 'KRT': '23', 'OMR': '24'
                    }
    return dict_stadium[code]

def create_race_result_url(race_code):
    # https://www.boatrace.jp/owpc/pc/race/racelist?rno=12&jcd=07&hd=20221124
    # 20222411FKO01
    # 3レターコードと場コードの対応表
    dict_stadium = {'KRY': '01', 'TDA': '02', 'EDG': '03', 'HWJ': '04',
                    'TMG': '05', 'HMN': '06', 'GMG': '07', 'TKN': '08',
                    'TSU': '09', 'MKN': '10', 'BWK': '11', 'SME': '12',
                    'AMG': '13', 'NRT': '14', 'MRG': '15', 'KJM': '16',
                    'MYJ': '17', 'TKY': '18', 'SMS': '19', 'WKM': '20',
                    'ASY': '21', 'FKO': '22', 'KRT': '23', 'OMR': '24'
                    }
    
    url = "https://www.boatrace.jp/owpc/pc/race/racelist"
    url = url + '?rno=' + race_code[11:14] + "&jcd=" + dict_stadium[race_code[8:11]] + "&hd="+race_code[0:8]
    return url
    
# データを分ける
def split_data(df, test_size=0.3):
    sorted_id_list = df.sort_values("日付").index.unique()
    train_id_list = sorted_id_list[: round(len(sorted_id_list) * (1-test_size))]
    test_id_list = sorted_id_list[round(len(sorted_id_list) * (1-test_size)) :]
    train = df.loc[train_id_list]
    test = df.loc[test_id_list]
    return train, test

# スタジアムのデータだけにする
def filter_stadium(df, stadium_code):
    df = df.copy()
    return df.filter(regex=stadium_code, axis=0)

# beginとendで囲って一時的に表示列数を変えたい
def begin_display_row_num(num=None):
    pd.set_option('display.max_rows', num)
    
def end_display_row_num():
    pd.set_option('display.max_rows', 60)