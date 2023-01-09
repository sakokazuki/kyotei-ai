from bs4 import BeautifulSoup
from requests import get
from time import sleep
import pandas as pd
import os
import csv
import importlib
import sys
from sklearn.metrics import roc_auc_score
from tqdm import tqdm
import numpy as np


# def gain(return_func, X, test, n_samples=100, lower=5000, min_threshold=0.5):
#     gain = {}
#     for i in tqdm(range(n_samples)):
#         threshold = 1 * i / n_samples + min_threshold * (1-(i/n_samples))
#         n_bets, money = return_func(X, test, threshold)
#         if n_bets > lower:
#             # print(n_bets, money)
#             gain[n_bets] = (n_bets*100 + money) / (n_bets*100)
#     return pd.Series(gain)


# 2連単の買い目をテーブル化する
#	                艇番 pred 　　　bet	  exacta 		
#   20220528OMR06	1	 1.300359	1	   1-3
#   .....
#　 の形のテーブルになる
def pred_table_exacta_top_2(pred_table):
    # 新しくpred_tableをコピーしexacta行をつける
    exacta_pred_table = pred_table.copy()
    exacta_pred_table['exacta'] = ''

    # レースコードの数だけ試行
    for race_code in exacta_pred_table.index.unique():

        # レースコードごとのpred_table
        race_pred_table = pred_table.filter(regex=race_code, axis=0)
        if(len(race_pred_table) >= 2):
            # print(race_pred_table.sort_values('pred', ascending=False))
            # pred値が高い順にソートし、top2つだけ選ぶ
            df = race_pred_table.sort_values('pred', ascending=False)[:2]
            exacta_bet = df['艇番'].tolist()

            exacta_bet_str = '-'.join(map(str, exacta_bet))
            data = [''] * len(race_pred_table)
            data[0] = exacta_bet_str
            exacta_pred_table.loc[race_code, 'exacta'] = data

    exacta_pred_table = exacta_pred_table[exacta_pred_table['exacta'] != '']
    return exacta_pred_table

# 2連単
def exacta_payoff_list(df):
    exacta_1 = df[  
            (df['2連単_艇番']==df['exacta']) | 
            (df['2連単_艇番']==-1)
        ].copy()
    exacta_1['return'] = exacta_1['2連単_払戻金']
    
    return exacta_1

# 2連単
def exacta_return(pred_table, return_table, buy_func=pred_table_exacta_top_2):
    df = return_table.copy() 
    
    pred_table = buy_func(pred_table.copy())
    df = df.merge(pred_table, left_index=True, right_index=True, how='right')
    
    n_bets = len(df)
    n_races = df.index.nunique()
    
    hit_df = exacta_payoff_list(df)
    
    n_hits = len(hit_df)
    
    money = -100 * n_bets
    money += hit_df['return'].sum()
    
    std = hit_df['return'].groupby(level=0).sum().std() * np.sqrt(n_races) / (100 * n_bets)
    
    return n_bets, money, n_hits, std

    
# 複勝
def place_payoff_list(df):
    place_1 = df[  
            (df['複勝_1着_艇番']==df['艇番']) | 
            (df['複勝_1着_艇番']==-1)
        ].copy()
    place_1['return'] = place_1['複勝_1着_払戻金']
    
    place_2 = df[df['複勝_2着_艇番']==df['艇番']].copy()
    place_2['return'] = place_2['複勝_2着_払戻金']
    
    return pd.concat([place_1, place_2])
    

# 複勝
def place_return(pred_table, return_table):
    n_bets = len(pred_table)
    n_races = pred_table.index.nunique()
    money = -100 * n_bets
    df = return_table.copy() 
    df = df.merge(pred_table, left_index=True, right_index=True, how='right')
    
    hit_df = place_payoff_list(df)
    
    n_hits = len(hit_df)
    money += hit_df['return'].sum()
    std = hit_df['return'].groupby(level=0).sum().std() * np.sqrt(n_races) / (100 * n_bets)

    return n_bets, money, n_hits, std

# 単勝
def win_payoff_list(df):
    # retval = []
    df = df[  
            (df['単勝_艇番']==df['艇番']) | 
            (df['単勝_艇番']==-1)
        ].copy()
    
    df['return'] = df['単勝_払戻金']
    return df
    
# 単勝
def win_return(pred_table, return_table):
    n_bets = len(pred_table)
    n_races = pred_table.index.nunique()
    money = -100 * n_bets
    df = return_table.copy() 
    df = df.merge(pred_table, left_index=True, right_index=True, how='right')
    
    
    hit_df = win_payoff_list(df)
    
    n_hits = len(hit_df)
    money += hit_df['return'].sum()
    std = hit_df['return'].groupby(level=0).sum().std() * np.sqrt(n_races) / (100 * n_bets)
    
        
    return n_bets, money, n_hits, std
    
    
class Gain2:
    def __init__(self, models, stadium_code, return_table, n_samples=100, lower=5000, t_range=[0.5, 3.5]):
        model_r1 = models.models[stadium_code+'_rank1']
        model_r3 = models.models[stadium_code+'_rank3']
        
        print(model_r1)
        pred_tables = {}
        for i in tqdm(range(n_samples)):
            threshold = t_range[1] * i / n_samples + t_range[0] * (1-(i/n_samples))
            pred_tables[threshold] = model_r1.mev.pred_table(model_r1.X_test, threshold)
        self.pred_tables_r1 = pred_tables
        
        pred_tables = {}
        for i in tqdm(range(n_samples)):
            threshold = t_range[1] * i / n_samples + t_range[0] * (1-(i/n_samples))
            pred_tables[threshold] = model_r3.mev.pred_table(model_r3.X_test, threshold)
        self.pred_tables_r3 = pred_tables
        
        self.return_table = return_table
        self.lower = lower
        
    def calc_return(self, return_func):
        gain = {}
        for threshold in tqdm(self.pred_tables_r1):
            pred_table = self.pred_tables_r1[threshold]
            n_races = pred_table.index.nunique()
            
            n_bets, money, n_hits, std = return_func(pred_table, self.return_table)
            if n_bets > self.lower:
                return_rate = (n_bets*100+money) / (n_bets*100)
                gain[threshold] = {'return_rate' : return_rate,
                                'n_hits': n_hits,
                                'hit_rate': n_hits/n_races,
                                'std' : std,
                                'n_bets': n_bets} 
        return pd.DataFrame(gain).T


# class Gain:
#     def __init__(self, mev, X, return_table, n_samples=100, lower=5000, t_range=[0.5, 3.5]):
#         pred_tables = {}
#         for i in tqdm(range(n_samples)):
#             threshold = t_range[1] * i / n_samples + t_range[0] * (1-(i/n_samples))
#             # pred_table = mev.pred_table(X, test, threshold)
#             pred_tables[threshold] = mev.pred_table(X, threshold)
        
#         self.mev = mev
#         self.pred_tables = pred_tables
#         self.return_table = return_table
#         self.lower = lower
        
#         # self.X = X
#         # self.test = test
        
#     def calc_return(self, return_func):
#         gain = {}
#         for threshold in tqdm(self.pred_tables):
#             pred_table = self.pred_tables[threshold]
#             n_races = pred_table.index.nunique()
            
#             n_bets, money, n_hits, std = return_func(pred_table, self.return_table)
#             if n_bets > self.lower:
#                 return_rate = (n_bets*100+money) / (n_bets*100)
#                 gain[threshold] = {'return_rate' : return_rate,
#                                 'n_hits': n_hits,
#                                 'hit_rate': n_hits/n_races,
#                                 'std' : std,
#                                 'n_bets': n_bets} 
#         return pd.DataFrame(gain).T
        
    
class ModelEvaluator:
    def __init__(self, model):
        self.model = model
    
    # モデルからpredict値を取得し、std=Trueで標準偏差を計算してから0-1にmapする
    def predict_proba(self, X, std=True, minmax=False):
        proba = pd.Series(self.model.predict_proba(X)[:, 1], index=X.index)
        
        # 標準化: レース内で相対評価
        if std:
            standard_scaler = lambda x: (x - x.mean()) / x.std()
            proba = proba.groupby(level=0).transform(standard_scaler)
        
        if minmax:
            # min-max スケーリング
            proba = (proba - proba.min()) / (proba.max() - proba.min())
        return proba
    
    def predict_map(self, y_pred, threshold=0.5):
        return [0 if p<threshold else 1 for p in y_pred]
    
    # predict値をしきい値よりも高ければ1、低ければ0に変換する(購入するときは1、しないなら0)
    def predict(self, X, threshold=0.5):
        y_pred = self.predict_proba(X)
        return self.predict_map(y_pred, threshold)
    
    # aucスコアを表示
    def score(self, y_true, X):
        return roc_auc_score(y_true, self.predict_proba(X))
    
    # feature_importanceを表示
    def feature_importance(self, X, n_display=20):
        importances = pd.DataFrame({"features": X.columns, 
                                    "importance": self.model.feature_importances_})
        return importances.sort_values("importance", ascending=False)[:n_display]
    
    def pred_table(self, X, threshold=0.5, bet_only=True):
        pred_table = X.copy()[['艇番']]
        # pred_table['pred'] = self.predict(X, threshold)
        pred_table['pred'] = self.predict_proba(X)
        pred_table['bet'] = self.predict_map(pred_table['pred'], threshold) 
        
        if bet_only:
            return pred_table[pred_table['bet']==1]
        else:
            return pred_table
        
