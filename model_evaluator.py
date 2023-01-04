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


def gain(return_func, X, test, n_samples=100, lower=5000, min_threshold=0.5):
    gain = {}
    for i in tqdm(range(n_samples)):
        threshold = 1 * i / n_samples + min_threshold * (1-(i/n_samples))
        n_bets, money = return_func(X, test, threshold)
        if n_bets > lower:
            # print(n_bets, money)
            gain[n_bets] = (n_bets*100 + money) / (n_bets*100)
    return pd.Series(gain)

# 上から順に2つ2連複を買うpred_tableの作成
def pred_table_exacta_top_2(pred_table):
    # 新しくpred_tableをコピーしexacta行をつける
    exacta_pred_table = pred_table.copy()
    exacta_pred_table['exacta'] = ''

    # レースコードの数だけ試行
    for race_code in exacta_pred_table.index.unique():

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

# 単勝
def win_payoff_list(df):
    retval = []
    retval.append(df[df['単勝_艇番']==df['艇番']]['単勝_払戻金'])
    retval.append(df[df['単勝_艇番']==-1]['単勝_払戻金'])
    
    return retval

# 複勝
def place_payoff_list(df):
    retval = []
    
    # 複勝1着
    retval.append(df[df['複勝_1着_艇番']==df['艇番']]['複勝_1着_払戻金'])
    # 複勝2着
    retval.append(df[df['複勝_2着_艇番']==df['艇番']]['複勝_2着_払戻金'])
    # 特払い 
    retval.append(df[df['複勝_1着_艇番']==-1]['複勝_1着_払戻金'])
    
    return retval

# 2連単
def exacta_payoff_list(df):
    retval = []
    
    func = lambda _: _.map(lambda x: '-'.join(map(str, x)))
    evaluate_func = lambda x, y: x in y.split(',') 
    
    winning = []
    for i, row in df.iterrows():
        winning.append(row['2連単_艇番'] in row['exacta'])
    df['winning'] = winning 
    retval.append(df[df['winning']]['2連単_払戻金'])
       
    retval.append(df[df['2連単_艇番'] == -1]['2連単_払戻金'])
    return retval
    # df[df['winning'] == True][
    
    
    

    
# 単勝
def win_return(pred_table, return_table):
    n_bets = len(pred_table)
    money = -100 * n_bets
    df = return_table.copy() 
    df = df.merge(pred_table, left_index=True, right_index=True, how='right')

    payoff_list = win_payoff_list(df)
    for v in payoff_list:
        money += v.sum()
        
    return n_bets, money

# 複勝
def place_return(pred_table, return_table):
    n_bets = len(pred_table)
    money = -100 * n_bets
    df = return_table.copy() 
    df = df.merge(pred_table, left_index=True, right_index=True, how='right')
    
    payoff_list = place_payoff_list(df)
    for v in payoff_list:
        money += v.sum()

    return n_bets, money

# 2連単
def exacta_return(pred_table, return_table, buy_func=pred_table_exacta_top_2):
    df = return_table.copy() 
    
    df_p = buy_func(pred_table.copy())
    df = df.merge(df_p, left_index=True, right_index=True, how='right')
    
    n_bets = len(df)
    money = -100 * n_bets
    
    if n_bets == 0:
        return n_bets, money # 0, 0
       
    payoff_list = exacta_payoff_list(df)   
    for v in payoff_list:
        money += v.sum()
    
    return n_bets, money

class Gain:
    def __init__(self, mev, X, test, return_table, n_samples=100, lower=5000, min_threshold=0.5):
        pred_tables = {}
        for i in tqdm(range(n_samples)):
            threshold = 1 * i / n_samples + min_threshold * (1-(i/n_samples))
            # pred_table = mev.pred_table(X, test, threshold)
            pred_tables[threshold] = mev.pred_table(X, test, threshold)
        
        self.mev = mev
        self.pred_tables = pred_tables
        self.return_table = return_table
        self.lower = lower
        
        # self.X = X
        # self.test = test
        
    def calc_return(self, func, lower=5000):
        gain = {}
        for pred_table in tqdm(self.pred_tables.values()):
            
            n_bets, money = func(pred_table, self.return_table)
            if n_bets > self.lower:
                gain[n_bets] = (n_bets*100+money) / (n_bets*100)
        return pd.Series(gain)
        
    
class ModelEvaluator:
    def __init__(self, model, return_table, std=True):
        self.model = model
        self.return_table = return_table.copy()
        self.std = std
    
    # def predict_proba(self, X):
    #     return self.model.predict_proba(X)[:, 1]
    
    # モデルからpredict値を取得し、std=Trueで標準偏差を計算してから0-1にmapする
    def predict_proba(self, X):
        proba = pd.Series(self.model.predict_proba(X)[:, 1], index=X.index)
        if self.std:
            standard_scaler = lambda x: (x - x.mean()) / x.std()
            proba = proba.groupby(level=0).transform(standard_scaler)
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
        pred_table['pred'] = self.predict(X, threshold)
        if bet_only:
            return pred_table[pred_table['pred']==1]['艇番']
        else:
            return pred_table
        
    # 予測データのしきい値を設定し、買う舟券のテーブルだけを見やすく作成する
    def pred_table(self, X, test, threshold=0.5, bet_only=True):
        pred_table = test.copy()[['艇番']]
        pred_table['pred'] = self.predict_proba(X)
        pred_table['bet'] = self.predict_map(pred_table['pred'], threshold) 
        if bet_only:
            return pred_table[pred_table['bet']==1]
        else:
            return pred_table
        
    def fukusho_return(self, X, threshold=0.5):
        pred_table = self.pred_table(X, threshold)
        n_bets = len(pred_table)
        money = -100 * len(pred_table)
        df = self.return_table.copy()
        
        df = df.merge(pred_table, left_index=True, right_index=True, how='right')
        
        # 複勝1着
        money += df[df['複勝_1着_艇番']==df['艇番']]['複勝_1着_払戻金'].sum()
        # 複勝2着
        money += df[df['複勝_2着_艇番']==df['艇番']]['複勝_2着_払戻金'].sum()
        # 特払い 
        money += df[df['複勝_1着_艇番']==-1]['複勝_1着_払戻金'].sum()
        return n_bets, money
    
    def tansho_return(self, X, threshold=0.5):
        pred_table = self.pred_table(X, threshold)
        n_bets = len(pred_table)
        money = -100 * n_bets
        df = self.return_table.copy()
        
        df = df.merge(pred_table, left_index=True, right_index=True, how='right')
        
        # 単勝1着
        money += df[df['単勝_艇番']==df['艇番']]['単勝_払戻金'].sum()
        # 特払い 
        money += df[df['単勝_艇番']==-1]['単勝_払戻金'].sum()
        return n_bets, money
