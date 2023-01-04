
from bs4 import BeautifulSoup
from requests import get
from time import sleep
import pandas as pd
import os
import csv
import importlib
import sys
import datetime as dt
from sklearn.preprocessing import LabelEncoder

class RaceResults:
    def __init__(self, data_table):
        self.data = data_table
        self.data_p = self._preprocessing(self.data)
        self.le_register = LabelEncoder().fit(self.data_p['登録番号'])
        
    def generate_categorical(self, rank_map_func=lambda x: 1 if x<2 else 0):
        retval = self.data_p.copy()
        # 1位になるのを1、それ以外を0に
        retval['rank'] = retval['着順'].map(rank_map_func)
        retval.drop(['着順'], axis=1, inplace=True)
        
        # ラベルエンコーディング
        retval['登録番号'] = self.le_register.transform(retval['登録番号'])
        # retval['登録番号'] = LabelEncoder().fit_transform(retval['登録番号'])
        # ダミー変数化 
        retval = pd.get_dummies(retval)
        # カテゴリ型に変換
        retval['登録番号'] = retval['登録番号'].astype('category')
        return retval 
        
    
    # 前処理をする関数
    def _preprocessing(self, data):
        retval = data.copy()
        # 着順に数字が入っていないものを学習データから省く
        retval = retval[~retval['着順'].astype(str).str.contains(r"\D")]
        retval = retval.copy()
        retval['着順'] = retval['着順'].astype(int)
        retval['年齢'] = retval['年齢'].astype(int)
        # 出走データはfloatで取得されるので予めfloatに変換
        retval['体重'] = retval['体重'].astype(float)

        # 出走表にはないデータを落とす
        retval.drop(['進入コース', 'スタートタイミング','レースタイム', '決まり手', 'スタジアムコード'], axis=1, inplace=True)
        # 直前情報はまだプログラムできていないので落とす
        retval.drop(['展示タイム', '天候', '風向', '風速(m)', '波の高さ(cm)'], axis=1, inplace=True)
        
        # 不必要だと感じたデータを落とす
        retval.drop(['距離(m)', '選手名'], axis=1, inplace=True)
        retval.drop(['コース別_1着率_all', 'コース別_1着率_10', 'コース別_3着率_all', 'コース別_3着率_10'], axis=1, inplace=True)
        
        # レースコードをindexとして使う
        retval = retval.set_index('レースコード')

        return retval
    
        
        
        