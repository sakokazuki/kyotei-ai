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
import utils

    
# # 複勝
# def place_payoff_list(df):
#     place_1 = df[  
#             (df['複勝_1着_艇番']==df['艇番']) | 
#             (df['複勝_1着_艇番']==-1) |
#             (df['複勝_1着_艇番']==-2)
#         ].copy()
#     place_1['return'] = place_1['複勝_1着_払戻金']
    
#     place_2 = df[df['複勝_2着_艇番']==df['艇番']].copy()
#     place_2['return'] = place_2['複勝_2着_払戻金']
    
#     return pd.concat([place_1, place_2])
    

# # 複勝
# def place_return(pred_table, return_table):
#     n_bets = len(pred_table)
#     n_races = pred_table.index.nunique()
#     money = -100 * n_bets
#     df = return_table.copy() 
#     df = df.merge(pred_table, left_index=True, right_index=True, how='right')
    
#     hit_df = place_payoff_list(df)
    
#     n_hits = len(hit_df)
#     money += hit_df['return'].sum()
#     std = hit_df['return'].groupby(level=0).sum().std() * np.sqrt(n_races) / (100 * n_bets)

#     return n_bets, money, n_hits, std



#---------------------------------------------------------------------------------------------------
# =============================================================
# 2連単
# =============================================================

# 2連単返金リスト作成
def create_exacta_return_list(pred_table, return_table):
    pred_table = pred_table.drop(['bet'], axis=1)
    # 返金テーブルとマージ
    pred_table = return_table.merge(pred_table, left_index=True, right_index=True, how='right')
    # 的中のみに絞る 
    win_table = exacta_payoff_list(pred_table)[['exacta', 'return']]
    # 的中してないものとマージ
    df = pd.merge(pred_table['exacta'], win_table, on=['レースコード', 'exacta'], how='left')
    
    # レースコードからutlを追加する処理
    n = pd.DataFrame(df.index)
    n = n.set_index(df.index)
    to_url = lambda x: utils.create_race_result_url(x)
    n['レースコード'] = n['レースコード'].map(to_url)
    df['url'] = n
    
    return df


# 2連単
def exacta_payoff_list(df):
    exacta_1 = df[  
            (df['2連単_艇番']==df['exacta']) | 
            (df['2連単_艇番']==-1) |
            (df['2連単_艇番']==-2)
        ].copy()
    exacta_1['return'] = exacta_1['2連単_払戻金']
    
    return exacta_1


def exacta_return_new(pred_table, return_table):
    n_bets = len(pred_table)
    n_races = pred_table.index.nunique()
    money = -100 * n_bets
    df = return_table.copy() 
    df = df.merge(pred_table, left_index=True, right_index=True, how='right')
    
    hit_df = exacta_payoff_list(df)
    
    n_hits = len(hit_df)
    money += hit_df['return'].sum()
    std = hit_df['return'].groupby(level=0).sum().std() * np.sqrt(n_races) / (100 * n_bets)
    
    return n_bets, money, n_hits, std

# 2連単
# 2連単の買い目をテーブル化する
#	                艇番 pred 　　　bet	  exacta 		
#   20220528OMR06	1	 1.300359	1	   1-3
#   .....
#　 の形のテーブルになる
def exacta_buy_func2(pred_table):
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
def exacta_buy_func1(r1, r3):
    df = pd.DataFrame()
    r1 = r1.rename(columns={'艇番': '艇番1'})
    r3 = r3.rename(columns={'艇番': '艇番2'})
    # 同じカラム名があるとエラになることがあり、落とす
    r1 = r1.drop(['pred', 'bet'], axis=1)
    try: 
        df = r1.join([r3], how='outer')
    except ValueError as e:
        print("value error!")
        print(e)
        return False

    # 片方にしか存在しないレースコードで欠損値がでるので欠損値を省く
    df = df[(~df['艇番1'].isna()) & (~df['艇番2'].isna())]
    df['艇番1'] = df['艇番1'].astype(int)
    df['艇番2'] = df['艇番2'].astype(int)

    # 重複削除
    df = df[~(df['艇番1'] == df['艇番2'])]
    # 掛け目を追加
    df['exacta'] = df['艇番1'].astype(str) + '-' + df['艇番2'].astype(str)
    return df


# 2連単
def exacta_return(self_gain):
    # 1着以内に入るpred_table閾値決め打ちで作成
    threshold_r1 = 1.9
    pred_table_r1 = pd.DataFrame()
    for threshold in self_gain.pred_tables_r1:
        if threshold > threshold_r1:
            pred_table_r1 = self_gain.pred_tables_r1[threshold].copy()
            break


    gain = {}
    return_list = {}
    # 3着以内に入るpred_tableについて閾値を決めて検証
    for threshold in tqdm(self_gain.pred_tables_r3):
        if len(pred_table_r1) == 0:
            continue
        pred_table_r3 = self_gain.pred_tables_r3[threshold].copy()
        # 2通りのかけ方
        pred_table = exacta_buy_func1(pred_table_r1, pred_table_r3)
        # pred_table = exacta_buy_func2(pred_table_r3)

        # エラーが出たときに原因のテーブルを返す
        if type(pred_table) is bool and pred_table == False:
            return pred_table_r1, pred_table_r3

        n_races = pred_table.index.nunique()

        n_bets, money, n_hits, std = exacta_return_new(pred_table, self_gain.return_table)
        if n_bets > self_gain.lower:
            return_rate = (n_bets*100+money) / (n_bets*100)
            gain[threshold] = {'return_rate' : return_rate,
                            'n_hits': n_hits,
                            'n_races': n_races,
                            'hit_rate': n_hits/n_races,
                            'std' : std,
                            'n_bets': n_bets} 
            return_list[threshold] = create_exacta_return_list(pred_table, self_gain.return_table)
    return pd.DataFrame(gain).T, return_list

# 2連単 1頭で買わない
def exacta_remove1R_return(self_gain):
    # 1着以内に入るpred_table閾値決め打ちで作成
    threshold_r1 = 0.2
    pred_table_r1 = pd.DataFrame()
    for threshold in self_gain.pred_tables_r1:
        if threshold > threshold_r1:
            pred_table_r1 = self_gain.pred_tables_r1[threshold].copy()
            break
    pred_table_r1 = pred_table_r1[~(pred_table_r1['艇番'] == 1)]


    gain = {}
    return_list = {}
    # 3着以内に入るpred_tableについて閾値を決めて検証
    for threshold in tqdm(self_gain.pred_tables_r3):
        if len(pred_table_r1) == 0:
            continue
        pred_table_r3 = self_gain.pred_tables_r3[threshold].copy()
        # 2通りのかけ方
        pred_table = exacta_buy_func1(pred_table_r1, pred_table_r3)
        # pred_table = exacta_buy_func2(pred_table_r3)

        # エラーが出たときに原因のテーブルを返す
        if type(pred_table) is bool and pred_table == False:
            return pred_table_r1, pred_table_r3.copy()

        n_races = pred_table.index.nunique()

        n_bets, money, n_hits, std = exacta_return_new(pred_table, self_gain.return_table)
        if n_bets > self_gain.lower:
            return_rate = (n_bets*100+money) / (n_bets*100)
            gain[threshold] = {'return_rate' : return_rate,
                            'n_hits': n_hits,
                            'n_races': n_races,
                            'hit_rate': n_hits/n_races,
                            'std' : std,
                            'n_bets': n_bets} 
            return_list[threshold] = create_exacta_return_list(pred_table, self_gain.return_table)
    return pd.DataFrame(gain).T, return_list

# =============================================================
# 3連単
# =============================================================

# 3連単返金リスト作成
def create_trifecta_return_list(pred_table, return_table):
    pred_table = pred_table.drop(['bet'], axis=1)
    # 返金テーブルとマージ
    pred_table = return_table.merge(pred_table, left_index=True, right_index=True, how='right')
    # 的中のみに絞る 
    win_table = trifecta_payoff_list(pred_table)[['trifecta', 'return']]
    # 的中してないものとマージ
    df = pd.merge(pred_table['trifecta'], win_table, on=['レースコード', 'trifecta'], how='left')
    
    # レースコードからutlを追加する処理
    n = pd.DataFrame(df.index)
    n = n.set_index(df.index)
    to_url = lambda x: utils.create_race_result_url(x)
    n['レースコード'] = n['レースコード'].map(to_url)
    df['url'] = n
    
    return df

# 3連単
def trifecta_payoff_list(df):
    trifecta_1 = df[  
            (df['3連単_艇番']==df['trifecta']) | 
            (df['3連単_艇番']==-1) |
            (df['3連単_艇番']==-2)
        ].copy()
    trifecta_1['return'] = trifecta_1['3連単_払戻金']
    
    return trifecta_1

# 3連単
def trifecta_return_new(pred_table, return_table):
    n_bets = len(pred_table)
    n_races = pred_table.index.nunique()
    money = -100 * n_bets
    df = return_table.copy() 
    df = df.merge(pred_table, left_index=True, right_index=True, how='right')
    
    hit_df = trifecta_payoff_list(df)
    
    n_hits = len(hit_df)
    money += hit_df['return'].sum()
    std = hit_df['return'].groupby(level=0).sum().std() * np.sqrt(n_races) / (100 * n_bets)
    
    return n_bets, money, n_hits, std

# 3連単 
def trifecta_buy_func1(r1, r3):
    df = pd.DataFrame()
    r1_1 = r1.rename(columns={'艇番': '艇番1'})
    r3_1 = r3.rename(columns={'艇番': '艇番2'})
    r3_2 = r3.rename(columns={'艇番': '艇番3'})

    # 同じカラム名があるとエラになることがあり、落とす
    r1_1 = r1_1.drop(['pred', 'bet'], axis=1)
    r3_1 = r3_1.drop(['pred', 'bet'], axis=1)

    # 2着,3着をマージ
    df = r1_1.join([r3_1], how='outer')
    df = df.join([r3_2], how='outer')

    # 片方にしか存在しないレースコードで欠損値がでるので欠損値を省く
    df = df[(~df['艇番1'].isna()) & (~df['艇番2'].isna()) & (~df['艇番3'].isna())]

    # int型にならないことがあるので型変換
    df['艇番1'] = df['艇番1'].astype(int)
    df['艇番2'] = df['艇番2'].astype(int)
    df['艇番3'] = df['艇番3'].astype(int)

    # 重複削除
    df = df[~(df['艇番1'] == df['艇番2'])]
    df = df[~(df['艇番1'] == df['艇番3'])]
    df = df[~(df['艇番2'] == df['艇番3'])]

    # 掛け目を追加
    df['trifecta'] = df['艇番1'].astype(str) + '-' + df['艇番2'].astype(str) + '-' + df['艇番3'].astype(str)
    return df

# 3連単
def trifecta_return(self_gain):
    # 1着以内に入るpred_table閾値決め打ちで作成
    threshold_r1 = 1.9
    pred_table_r1 = pd.DataFrame()
    for threshold in self_gain.pred_tables_r1:
        if threshold > threshold_r1:
            pred_table_r1 = self_gain.pred_tables_r1[threshold].copy()
            break

    gain = {}
    return_list = {}
    # 3着以内に入るpred_tableについて閾値を決めて検証
    for threshold in tqdm(self_gain.pred_tables_r3):
        if len(pred_table_r1) == 0:
            continue
        pred_table_r3 = self_gain.pred_tables_r3[threshold].copy()

        pred_table = trifecta_buy_func1(pred_table_r1, pred_table_r3)

        # エラーが出たときに原因のテーブルを返す
        if type(pred_table) is bool and pred_table == False:
            return pred_table_r1, pred_table_r3

        n_races = pred_table.index.nunique()

        n_bets, money, n_hits, std = trifecta_return_new(pred_table, self_gain.return_table)
        if n_bets > self_gain.lower:
            return_rate = (n_bets*100+money) / (n_bets*100)
            gain[threshold] = {'return_rate' : return_rate,
                            'n_hits': n_hits,
                            'n_races': n_races,
                            'hit_rate': n_hits/n_races,
                            'std' : std,
                            'n_bets': n_bets} 
            return_list[threshold] = create_trifecta_return_list(pred_table, self_gain.return_table)
    return pd.DataFrame(gain).T , return_list

# 3連単で1頭買わない
def trifecta_remove1R_return(self_gain):
    # 1着以内に入るpred_table閾値決め打ちで作成
    threshold_r1 = 0.2 # 買い目が減っても良ければ0.8なども
    pred_table_r1 = pd.DataFrame()
    for threshold in self_gain.pred_tables_r1:
        if threshold > threshold_r1:
            pred_table_r1 = self_gain.pred_tables_r1[threshold].copy()
            break
    pred_table_r1 = pred_table_r1[~(pred_table_r1['艇番'] == 1)]

    gain = {}
    return_list = {}
    # 3着以内に入るpred_tableについて閾値を決めて検証
    for threshold in tqdm(self_gain.pred_tables_r3):
        if len(pred_table_r1) == 0:
            continue
        pred_table_r3 = self_gain.pred_tables_r3[threshold].copy()

        pred_table = trifecta_buy_func1(pred_table_r1, pred_table_r3)

        # エラーが出たときに原因のテーブルを返す
        if type(pred_table) is bool and pred_table == False:
            return pred_table_r1, pred_table_r3

        n_races = pred_table.index.nunique()

        n_bets, money, n_hits, std = trifecta_return_new(pred_table, self_gain.return_table)
        if n_bets > self_gain.lower:
            return_rate = (n_bets*100+money) / (n_bets*100)
            gain[threshold] = {'return_rate' : return_rate,
                            'n_hits': n_hits,
                            'n_races': n_races,
                            'hit_rate': n_hits/n_races,
                            'std' : std,
                            'n_bets': n_bets} 
            return_list[threshold] = create_trifecta_return_list(pred_table, self_gain.return_table)
    return pd.DataFrame(gain).T, return_list

# =============================================================
# 単勝 
# =============================================================

# 単勝返金リスト作成
def create_win_return_list(pred_table, return_table):
    pred_table = pred_table.drop(['bet'], axis=1)
    # 返金テーブルとマージ
    pred_table = return_table.merge(pred_table, left_index=True, right_index=True, how='right')
    # 的中のみに絞る 
    win_table = win_payoff_list(pred_table)[['win', 'return']]
    # 的中してないものとマージ
    df = pd.merge(pred_table['win'], win_table, on=['レースコード', 'win'], how='left')
    
    # レースコードからutlを追加する処理
    n = pd.DataFrame(df.index)
    n = n.set_index(df.index)
    to_url = lambda x: utils.create_race_result_url(x)
    n['レースコード'] = n['レースコード'].map(to_url)
    df['url'] = n
    
    return df
    

# 単勝
def win_payoff_list(df):
    # retval = []
    df = df[  
            (df['単勝_艇番']==df['win']) | 
            (df['単勝_艇番']==-1)
        ].copy()
    
    df['return'] = df['単勝_払戻金']
    return df
    
# 単勝
def win_return_new(pred_table, return_table):
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

# 単勝1着のモデル
def win_r1_return(self_gain):
    gain = {}
    return_list = {}
    for threshold in tqdm(self_gain.pred_tables_r1):
        pred_table = self_gain.pred_tables_r1[threshold].copy()
        pred_table['win'] = pred_table['艇番']
        n_races = pred_table.index.nunique()

        n_bets, money, n_hits, std = win_return_new(pred_table, self_gain.return_table)
        if n_bets > self_gain.lower:
            return_rate = (n_bets*100+money) / (n_bets*100)
            gain[threshold] = {'return_rate' : return_rate,
                            'n_hits': n_hits,
                            'n_races': n_races,
                            'hit_rate': n_hits/n_races,
                            'std' : std,
                            'n_bets': n_bets} 
            return_list[threshold] = create_win_return_list(pred_table, self_gain.return_table)
    return pd.DataFrame(gain).T, return_list

# 単勝1着以内、1号艇を買わない
def win_r1_remove1R_return(self_gain):
    gain = {}
    return_list = {}
    for threshold in tqdm(self_gain.pred_tables_r1):
        pred_table = self_gain.pred_tables_r1[threshold].copy()
        pred_table['win'] = pred_table['艇番']
        # 1号艇を除く
        pred_table = pred_table[~(pred_table['艇番'] == 1)]
        
        n_races = pred_table.index.nunique()

        n_bets, money, n_hits, std = win_return_new(pred_table, self_gain.return_table)
        if n_bets > self_gain.lower:
            return_rate = (n_bets*100+money) / (n_bets*100)
            gain[threshold] = {'return_rate' : return_rate,
                            'n_hits': n_hits,
                            'n_races': n_races,
                            'hit_rate': n_hits/n_races,
                            'std' : std,
                            'n_bets': n_bets} 
            return_list[threshold] = create_win_return_list(pred_table, self_gain.return_table)
            
    return pd.DataFrame(gain).T, return_list


# 単勝3着以内のモデル
def win_r3_return(self_gain):
    gain = {}
    return_list = {}
    for threshold in tqdm(self_gain.pred_tables_r3):
        pred_table = self_gain.pred_tables_r3[threshold].copy()
        pred_table['win'] = pred_table['艇番']
        n_races = pred_table.index.nunique()

        n_bets, money, n_hits, std = win_return_new(pred_table, self_gain.return_table)
        if n_bets > self_gain.lower:
            return_rate = (n_bets*100+money) / (n_bets*100)
            gain[threshold] = {'return_rate' : return_rate,
                            'n_hits': n_hits,
                            'n_races': n_races,
                            'hit_rate': n_hits/n_races,
                            'std' : std,
                            'n_bets': n_bets} 
            return_list[threshold] = create_win_return_list(pred_table, self_gain.return_table)
    return pd.DataFrame(gain).T, return_list

# =============================================================
# ユーティリティ 
# =============================================================
def show_return_table(preds, target_threshold, hit_only=True):
    pred = pd.DataFrame()
    for threshold in preds:
        if threshold > target_threshold:
            pred = preds[threshold]
            break
            
            
    if pred.empty:
        return pred
    if hit_only:
        return pred[~pred['return'].isna()]
    return pred

    
class Gain2:
    def __init__(self, models, stadium_code, return_table, n_samples=100, lower=5000, t_range_r1=[0.5,3.5], t_range_r3=[0, 2]):
        model_r1 = models.models[stadium_code+'_rank1']
        model_r3 = models.models[stadium_code+'_rank3']
        
        pred_tables = {}
        for i in tqdm(range(n_samples)):
            threshold = t_range_r1[1] * i / n_samples + t_range_r1[0] * (1-(i/n_samples))
            pred_tables[threshold] = model_r1.mev.pred_table(model_r1.X_test, threshold)
        self.pred_tables_r1 = pred_tables
        # self.pred_tables_r1 = 
        
        pred_tables = {}
        for i in tqdm(range(n_samples)):
            threshold = t_range_r3[1] * i / n_samples + t_range_r3[0] * (1-(i/n_samples))
            pred_tables[threshold] = model_r3.mev.pred_table(model_r3.X_test, threshold)
        self.pred_tables_r3 = pred_tables
        
        self.return_table = return_table
        self.lower = lower
        
        # 全レースのpred_tableも保管
        self.pred_table_r1_all = model_r1.mev.pred_table(model_r1.X_test, 0, False)
        self.pred_table_r3_all = model_r3.mev.pred_table(model_r3.X_test, 0, False)
        # 全レース数をpred_tableのレースコードの数から求める
        self.n_all_races = model_r1.mev.pred_table(model_r1.X_test, 0, True).index.nunique()
        
        
    
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
        
