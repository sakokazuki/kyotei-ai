
from bs4 import BeautifulSoup
from requests import get
from time import sleep
import pandas as pd
import os
import csv
import importlib
import sys
import datetime as dt

class ReturnTable:
    def __init__(self, data_table):
        self.data = data_table
        self.data_p = self._preprocessing(self.data)
        
    
    # 返金テーブルの前処理(型変換など)
    def _preprocessing(self, data):
        retval = data.copy()
        # 艇番 特払い: -1 / 不成立: -2 / 無投票: 0
#         retval = retval.fillna({'複勝_1着_払戻金': 100, '複勝_2着_払戻金': 100, '複勝_1着_艇番': 0, '複勝_2着_艇番': 0})
#         retval['単勝_艇番'] = retval['単勝_艇番'].apply(lambda x: -1 if x == 'い' else x)
#         retval['単勝_艇番'] = retval['単勝_艇番'].apply(lambda x: -2 if x == '不' else x)
#         retval['複勝_1着_艇番'] = retval['複勝_1着_艇番'].apply(lambda x: -1 if x == 'い' else x)
#         retval['複勝_1着_艇番'] = retval['複勝_1着_艇番'].apply(lambda x: -2 if x == '不' else x)
#         retval['複勝_2着_艇番'] = retval['複勝_2着_艇番'].apply(lambda x: -1 if x == 'い' else x)
#         retval['複勝_2着_艇番'] = retval['複勝_2着_艇番'].apply(lambda x: -2 if x == '不' else x)
#         retval['複勝_2着_艇番'] = retval['複勝_2着_艇番'].apply(lambda x: 0 if x == ' ' else x)

#         retval['単勝_艇番'] = retval['単勝_艇番'].astype(int)
#         retval['複勝_1着_艇番'] = retval['複勝_1着_艇番'].astype(int)
#         retval['複勝_2着_艇番'] = retval['複勝_2着_艇番'].astype(int)
#         retval['複勝_1着_払戻金'] = retval['複勝_1着_払戻金'].astype(int)
#         retval['複勝_2着_払戻金'] = retval['複勝_2着_払戻金'].astype(int)
        
        # レースコードをindexとして使う
        retval = retval.set_index('レースコード')
        return retval