import os
import re
import datetime as dt
import numpy as np
import pandas as pd
import utils
from tqdm import tqdm
class RacerTable:
    # race_table: データベース
    # target_table: 計算するターゲット
    # saved_data: すでに計算済みのもの
    # need_update: 強制的に全部計算しなおすか
    def __init__(self, race_table, target_table, saved_data = pd.DataFrame(), need_update=False):
        # サンプル数リスト
        self.samples_list = ['all', 10, 5]
        # 着率や平均の計算をする関数のマップ
        self.target_func_map = {
            '1着率':             lambda df: 0 if len(df) == 0 else len(df[df['着順'] == 1])/len(df),
            '3着率':             lambda df: 0 if len(df) == 0 else len(df[(0<df['着順']) & (df['着順']<=3)])/len(df),
            '着順平均':          lambda df: df['着順'].mean(),
            'スタートタイミング': lambda df: df['スタートタイミング'].mean(),
            '展示タイム':         lambda df: df['展示タイム'].mean(),
                      }
        # dfをフィルターする関数のマップ
        self.filtered_df_names = ['全国', '当地', 'コース別']
       
        # メインのテーブル作成処理
        racer_table = self.create_racer_table(race_table, target_table, saved_data, need_update)
        
        self.data = racer_table
        # 使用雨する行のみに絞る
        column_names = self.create_racer_columnname_list()
        self.data_p = self.data[column_names]
    
    # picleファイルからクラスを作成
    @classmethod
    def create_from_pickle(cls, race_table, pic_name):
        try:
            saved_racer_table = pd.read_pickle(pic_name)
        except (OSError, IOError) as e:
            saved_racer_table = pd.DataFrame()
        
        retval = cls(race_table, race_table, saved_racer_table)
        retval.data.to_pickle(pic_name)
        return retval
    
    # 出走テーブルからクラスを作成
    @classmethod
    def create_from_syusso(cls, race_table, syusso_table):
        retval = cls(race_table, syusso_table)
        return retval
    
    
    # カラム名リストを事前に作成する
    def create_racer_columnname_list(self):
        retval = ['レースコード', '艇番'] # この後にリストに追加される
        for n_samples in self.samples_list:
            for row_name in self.filtered_df_names:
                for target_func_key in self.target_func_map:
                    label = '{}_{}_{}R'.format(row_name, target_func_key, n_samples)
                    retval.append(label)
        return retval
        
    # レーステーブルをレーサーごとに分ける
    def create_racer_array(self, result_table):
        li = {}
        for no in result_table['登録番号'].unique():
            li[no] = result_table[result_table['登録番号'] == no]
        return li
    
    # メインの処理
    def create_racer_table(self, source_df, target_df, saved_data, need_update):
        retval = saved_data
        if need_update == False and len(saved_data) > 0:
            retval = saved_data
            # 既存のテーブルに含まれていないレースコードのデータのみ抽出
            target_df = target_df[~target_df["レースコード"].isin(retval['レースコード'])]


        target = self.create_racer_array(target_df) 
        source = self.create_racer_array(source_df)
        if len(target) == 0 :
            return retval
        print("レーサーの過去生成データテーブルの作成")
        for racer_id in tqdm(target):
            new_df = target[racer_id].copy()
            # 着順文字列を0にしてintへ
            source_racer = source[racer_id]
            source_racer = source[racer_id].copy()
            source_racer['着順'] = source_racer['着順'].map(lambda n: n if str(n).isnumeric() else 0).astype(int)

            for i, row in new_df.iterrows():

                # 対象となるレース以前の日付のみのデータに
                source_prev = source_racer[source_racer['日付'] < row['日付']]
                stadium_code = row['スタジアムコード']
                teiban = row['艇番']

                # all, 当地, コースごとのフィルターdataframeを作成
                all_df     = source_prev
                stadium_df = source_prev[source_prev['スタジアムコード'] == stadium_code]
                teiban_df  = source_prev[source_prev['艇番'] == teiban]
                # dfごとに処理するのでmap化
                filtered_df_map = {'全国': all_df, 
                                   '当地': stadium_df,
                                   'コース別': teiban_df,
                                   }
                diff_list = set(filtered_df_map) ^ set(self.filtered_df_names)
                if len(diff_list) > 0:
                    print('self.filtered_df_nameとfiltered_df_mapのkeyが一致している必要があります')
                

                # サンプル数ごと、dataframeごとに計算を行う
                # (len(filtered_df_map) * len(target_func_map) * len(samples_list)個の列が生成される)
                for n_samples in self.samples_list:
                    for row_name in self.filtered_df_names:
                        if n_samples == 'all':
                            source_filtered = filtered_df_map[row_name]
                        elif n_samples> 0:
                            source_filtered = filtered_df_map[row_name].sort_values('日付', ascending=False).head(n_samples)
                        else:
                            raise Exception('n_samples must be >0')

                        for target_func_key in self.target_func_map:
                            label = '{}_{}_{}R'.format(row_name, target_func_key, n_samples)
                            new_df.at[i, label] = self.target_func_map[target_func_key](source_filtered)


            retval = pd.concat([retval, new_df])
        return retval
    
    # レースデータとマージ
    def merge_racer_race(self, source):
        return pd.merge(source, self.data_p, on=['レースコード', '艇番'])