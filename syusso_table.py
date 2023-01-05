import os
import sys
# 時間を制御する time モジュールをインポート
from time import sleep
import pandas as pd
# HTTP通信ライブラリの requests モジュールをインポート
from requests import get
import csv
from bs4 import BeautifulSoup
import mojimoji
import numpy as np
import datetime as dt

import os
import create_tables
import utils

class SyussoTable:
    def __init__(self, date, stadium):
        # date = '20221122'
        # stadium = 'HWJ'
        pickle_name = 'syusso_'+date+stadium
        # pd.read_pickle(pickle_name)
        # print(pickle_name)
        # print(test)
        is_file = os.path.isfile(pickle_name)
        if is_file:
            self.data = pd.read_pickle(pickle_name)
        else:
            self.data = self._scrape(date, stadium)
            racer_table = self._create_racer_table(pd.read_pickle("pic_race_table"), self.data)
            racer_table = racer_table.drop(['スタジアムコード', '日付', '登録番号', '選手名', 'モーター番号', 'ボート番号', 
                                            '年齢', '支部', '体重', '級別', '全国勝率', '全国2連対率', '当地勝率', '当地2連対率',
                                            'モーター2連対率', 'ボート2連対率', 
                                            '今節成績_1-1', '今節成績_1-2',
                                            '今節成績_2-1', '今節成績_2-2',
                                            '今節成績_3-1', '今節成績_3-2',
                                            '今節成績_4-1', '今節成績_4-2',
                                            '今節成績_5-1', '今節成績_5-2',
                                            '今節成績_6-1', '今節成績_6-2',
                                           ], axis=1)
            self.data = pd.merge(self.data, racer_table, on=['レースコード', '艇番'])
            
            self.data.to_pickle(pickle_name)
        # 直前情報のスクレイピングとマージ 
        before_info = self._scrape_beforeinfo(date, stadium)
        if before_info.empty == False:
            self.data = pd.merge(self.data, before_info, on=['レースコード', '艇番'], how='left')
            self.data.to_pickle(pickle_name)
        
        
        # racer_table = create_tables.create_racer_table(self.data, pd.read_pickle("pic_racer_table"))
        
        # self.racer_t = racer_table
        self.data_p = self._preprocessing(self.data)
    
    # 出走表データのカテゴリ化
    def generate_categorical(self, race_results): # RaceResults
        df = self.data_p.copy()
        # 新規の選手番号を追加
        mask_register = df['登録番号'].isin(race_results.le_register.classes_)
        new_register_id = df['登録番号'].mask(mask_register).dropna().unique()
        race_results.le_register.classes_ = np.concatenate([race_results.le_register.classes_, new_register_id])
        df['登録番号'] =  race_results.le_register.transform(df['登録番号'])

        shibu = race_results.data_p['支部'].unique()
        grade = race_results.data_p['級別'].unique()
        df['支部'] = pd.Categorical(df['支部'], shibu)
        df['級別'] = pd.Categorical(df['級別'], grade)

        # 出走データのダミー変数化
        df = pd.get_dummies(df, columns=['支部', '級別'])
        df['登録番号'] = df['登録番号'].astype('category')
        return df
        
    def _preprocessing(self, data):
        df = data.copy()
        df['艇番'] = df['艇番'].astype(int)
        df['モーター番号'] = df['モーター番号'].astype(int)
        df['ボート番号'] = df['ボート番号'].astype(int)
        df['登録番号'] = df['登録番号'].astype(int)
        df['年齢'] = df['年齢'].astype(int)
        df['体重'] = df['体重'].astype(float)
        df['全国勝率'] = df['全国勝率'].astype(float)
        df['全国2連対率'] = df['全国2連対率'].astype(float)
        df['当地勝率'] = df['当地勝率'].astype(float)
        df['当地2連対率'] = df['当地2連対率'].astype(float)
        df['モーター2連対率'] = df['モーター2連対率'].astype(float)
        df['ボート2連対率'] = df['ボート2連対率'].astype(float)
        df['今節成績_1-1'] = pd.to_numeric(df["今節成績_1-1"], errors="coerce")
        df['今節成績_1-2'] = pd.to_numeric(df["今節成績_1-2"], errors="coerce")
        df['今節成績_2-1'] = pd.to_numeric(df["今節成績_2-1"], errors="coerce")
        df['今節成績_2-2'] = pd.to_numeric(df["今節成績_2-2"], errors="coerce")
        df['今節成績_3-1'] = pd.to_numeric(df["今節成績_3-1"], errors="coerce")
        df['今節成績_3-2'] = pd.to_numeric(df["今節成績_3-2"], errors="coerce")
        df['今節成績_4-1'] = pd.to_numeric(df["今節成績_4-1"], errors="coerce")
        df['今節成績_4-2'] = pd.to_numeric(df["今節成績_4-2"], errors="coerce")
        df['今節成績_5-1'] = pd.to_numeric(df["今節成績_5-1"], errors="coerce")
        df['今節成績_5-2'] = pd.to_numeric(df["今節成績_5-2"], errors="coerce")
        df['今節成績_6-1'] = pd.to_numeric(df["今節成績_6-1"], errors="coerce")
        df['今節成績_6-2'] = pd.to_numeric(df["今節成績_6-2"], errors="coerce")
        df.drop(['選手名', '日付', 'スタジアムコード'], axis=1, inplace=True)
        df.drop(['コース別_1着率_all', 'コース別_1着率_10', 'コース別_3着率_all', 'コース別_3着率_10'], axis=1, inplace=True)
        df = df.set_index('レースコード')
        
        # 直前情報がでているもののみにする
        df = df[~df['展示タイム'].isna()]
        return df
    

    # スクレイピングHTMLパース
    def _parse_html(self, tbody):
        n = 0
        retval = []
        for b in tbody:
            n = n+1

            # 艇番
            teiban = n

            tmp = b.select('div.is-fs11')[0].get_text()
            # 登録番号 
            register_no = tmp.split('/')[0].strip()
            # 級別
            grade = tmp.split('/')[1].strip()

            # 選手名
            tmp = b.select_one('div.is-fs18').select_one('a').get_text()
            name = tmp.strip()
            name= ''.join(name.split())

            # 支部/年齢/体重
            tmp = b.select('div.is-fs11')[1].get_text()
            shibu = tmp.split('\n')[0].split('/')[0].strip()
            age = tmp.split('\n')[1].split('/')[0].strip().replace('歳', '')
            weight = tmp.split('\n')[1].split('/')[1].strip().replace('kg', '')

            # 全国勝率/全国2連対率
            tmp = b.select('td.is-lineH2')[1].get_text()
            zenkoku_win = tmp.split('\n')[0].strip()
            zenkoku_2ren = tmp.split('\n')[1].strip()

            # 当地勝率/当地2連対率
            tmp = b.select('td.is-lineH2')[2].get_text()
            touti_win = tmp.split('\n')[0].strip()
            touti_2ren = tmp.split('\n')[1].strip()

            # モーター2連対率
            tmp = b.select('td.is-lineH2')[3].get_text()
            moter_no = tmp.split('\n')[0].strip()
            moter_2ren = tmp.split('\n')[1].strip()

            # ボート2連対率
            tmp = b.select('td.is-lineH2')[4].get_text()
            boat_no = tmp.split('\n')[0].strip()
            boat_2ren = tmp.split('\n')[1].strip()


            # この順番に入れる
            # 0   艇番        616799 non-null  int32         
            # 1   モーター番号    616799 non-null  int32         
            # 2   ボート番号     616799 non-null  int32         
            # 3   距離(m)     616799 non-null  int32         
            # 4   日付        616799 non-null  datetime64[ns]
            # 5   登録番号      616799 non-null  int64         
            # 6   年齢        616799 non-null  int64         
            # 7   支部        616799 non-null  object        
            # 8   体重        616799 non-null  int64         
            # 9   級別        616799 non-null  object        
            # 10  全国勝率      616799 non-null  float64       
            # 11  全国2連対率    616799 non-null  float64       
            # 12  当地勝率    616799 non-null  float64       
            # 13  当地2連対率    616799 non-null  float64       
            # 14  モーター2連対率  616799 non-null  float64       
            # 15  ボート2連対率   616799 non-null  float64       
            # 16  今節成績_1-1  494601 non-null  float64       
            # 17  今節成績_1-2  286070 non-null  float64       
            # 18  今節成績_2-1  385213 non-null  float64       
            # 19  今節成績_2-2  230503 non-null  float64       
            # 20  今節成績_3-1  275588 non-null  float64       
            # 21  今節成績_3-2  168860 non-null  float64       
            # 22  今節成績_4-1  166039 non-null  float64       
            # 23  今節成績_4-2  105326 non-null  float64       
            # 24  今節成績_5-1  75942 non-null   float64       
            # 25  今節成績_5-2  48224 non-null   float64       
            # 26  今節成績_6-1  6458 non-null    float64       
            # 27  今節成績_6-2  4280 non-null    float64       
            # 28  rank      616799 non-null  int64    
            data = []
            data.append(teiban)
            data.append(name)
            data.append(moter_no)
            data.append(boat_no)
            data.append(register_no)
            data.append(age)
            data.append(shibu)
            data.append(weight)
            data.append(grade)
            data.append(zenkoku_win)
            data.append(zenkoku_2ren)
            data.append(touti_win)
            data.append(touti_2ren)
            data.append(moter_2ren)
            data.append(boat_2ren)

            # 今節成績をループで代入
            tmp = b.select('tr')[3].select('td')
            for i in range(0,12):
                if i%2 == 0:
                    label = '今節成績_{}-1'.format(int(i/2)+1)
                else:
                    label = '今節成績_{}-2'.format(int(i/2)+1)
                value = tmp[i].get_text().strip()
                value =mojimoji.zen_to_han(value)
                data.append(value)

            retval.append(data)
        return retval

    # スクレイピングリクエスト
    def _get_syusso(self, url):
        print(url)
        table =  pd.read_html(url)
        html = get(url)
        soup = BeautifulSoup(html.content, 'html.parser')

        syusso_table = soup.find('div', class_='table1 is-tableFixed__3rdadd').select_one('table')
        tbody = syusso_table.select('tbody')

        return self._parse_html(tbody)
    
    # スクレイピング
    def _scrape(self, date, stadium):
        FIXED_URL = 'https://www.boatrace.jp/owpc/pc/race/racelist'
        INTERVAL=3
        
        syusso_table = pd.DataFrame()

        for r in range(1,13):
            race_round = str(r)
            race_code = date + stadium + race_round.zfill(2)
            
            stadium_code = utils.dict_stadium(stadium)

            target_url = FIXED_URL  + "?rno=" + race_round\
                     + "&jcd=" + stadium_code+ "&hd=" + date

            df_columns = ['艇番', '選手名', 'モーター番号', 'ボート番号', '登録番号', '年齢', '支部', '体重', '級別',
                           '全国勝率', '全国2連対率', '当地勝率', '当地2連対率', 'モーター2連対率', 'ボート2連対率', 
                           '今節成績_1-1', '今節成績_1-2', '今節成績_2-1', '今節成績_2-2', '今節成績_3-1', '今節成績_3-2',
                           '今節成績_4-1', '今節成績_4-2', '今節成績_5-1', '今節成績_5-2', '今節成績_6-1', '今節成績_6-2']
            race_syusso = self._get_syusso(target_url)
            
            race_date = dt.datetime.strptime(date, '%Y%m%d')
            race_date = race_date + dt.timedelta(hours=int(r))

            df = pd.DataFrame(race_syusso)
            df.columns = df_columns
            df['レースコード'] = race_code
            df['日付'] = race_date
            df['スタジアムコード'] = stadium_code
            # df.index = [race_code] * len(df)
            # print(df)

            syusso_table = pd.concat([syusso_table, df])

            # 指定した間隔をあける
            sleep(INTERVAL)
        return syusso_table
    
    # 直前情報のHTMLパース
    def _parse_html_beforeinfo(self, html):

        retval = []

        # 水温や風速など
        # 天候     
        weather     = html.find('div', class_='weather1_bodyUnit is-weather').find('span', 'weather1_bodyUnitLabelTitle').get_text()
        # 風速(m) 
        wind_speed  = html.find('div', class_='weather1_bodyUnit is-wind').find('span', 'weather1_bodyUnitLabelData').get_text()
        # 波の高さ(cm)
        wave_height = html.find('div', class_='weather1_bodyUnit is-wave').find('span', 'weather1_bodyUnitLabelData').get_text()
        # 風向
        direction_elem = html.find('div', class_='weather1_bodyUnit is-direction').find('p', 'weather1_bodyUnitImage')
        wind_direction_elem = html.find('div', class_='weather1_bodyUnit is-windDirection').find('p', 'weather1_bodyUnitImage')
        # 風向と会場の方向についてるクラス名で風の方角を決定できる
        direction_class = direction_elem['class'][1] # is-direction5
        wind_direction_class = wind_direction_elem['class'][1] # is-wind11
        # クラス名の数字のみ取り出す
        direction_no = int(direction_class.replace('is-direction', ''))
        wind_direction_no = int(wind_direction_class.replace('is-wind', ''))
        # 会場の向きと風向きの差分で方向が定る 
        wind_direction = (wind_direction_no - direction_no) % 16
        if wind_direction == 0:
            wind_direction_str = "南"
        if wind_direction == 2:
            wind_direction_str = "南西"
        if wind_direction == 4:
            wind_direction_str = "西"
        if wind_direction == 6:
            wind_direction_str = "北西"
        if wind_direction == 8:
            wind_direction_str = "北"
        if wind_direction == 10:
            wind_direction_str = "北東"
        if wind_direction == 12:
            wind_direction_str = "東"
        if wind_direction == 14:
            wind_direction_str = "南東"

        # print(weather, wind_speed, wave_height)
        # print(wind_direction_str)

        table = html.find('div', class_='grid is-type3 h-clear').select_one('table')
        tbody = table.select('tbody')

        for b in tbody:
            tr0 = b.select('tr')[0]
            td_teiban = b.select('td')[0]
            td_tenji_time = b.select('td')[4]

            teiban =  int(td_teiban.get_text())
            tenji_time = td_tenji_time.get_text()

            if tenji_time == '\xa0' :
                break

            data = []
            data.append(teiban)
            data.append(float(tenji_time))
            data.append(weather)
            data.append(wind_speed.replace('m', ''))
            data.append(wind_direction_str)
            data.append(wave_height.replace('cm', ''))

            retval.append(data)

        return retval
    
    # 直前情報リクエスト
    def _get_beforeinfo(self, url):
        print(url)
        table = pd.read_html(url)
        html = get(url)
        soup = BeautifulSoup(html.content, 'html.parser')
        return self._parse_html_beforeinfo(soup)
    
    # 直前情報スクレイピング
    def _scrape_beforeinfo(self, date, stadium):
        FIXED_URL = 'https://www.boatrace.jp/owpc/pc/race/beforeinfo'
        INTERVAL=3
        
        beforeinfo_table = pd.DataFrame()
        
        for r in range(1, 13):
            
            
            race_round = str(r)
            race_code = date + stadium + race_round.zfill(2)
            
            # すでに含まれていたらスクレイピングスキップ
            is_null = True
            if '展示タイム' in self.data:
                is_null = self.data[self.data['レースコード'] == race_code]['展示タイム'].isna().iloc[0]
            if is_null == False:
                print(race_code, '直前情報リクエスト済みなのでスキップ')
                continue
            
            stadium_code = utils.dict_stadium(stadium)
            
            target_url = FIXED_URL + "?rno=" + race_round\
                + "&jcd=" + stadium_code + "&hd=" + date
            # [[1, 6.81, '晴', '3', '北西', '3'],
            df_columns = ['艇番', '展示タイム', '天候', '風速(m)', '風向', '波の高さ(cm)']
            before_info = self._get_beforeinfo(target_url)
            if len(before_info) == 0:
                break

            df = pd.DataFrame(before_info)
            df.columns = df_columns
            df['レースコード'] = race_code
            
            beforeinfo_table = pd.concat([beforeinfo_table, df])
            
            # 指定した間隔をあける
            sleep(INTERVAL)
            
        return beforeinfo_table
        
    
    
    def _create_racer_array(self, result_table):
        li = {}
        for no in result_table['登録番号'].unique():
            li[no] = result_table[result_table['登録番号'] == no]
        return li
    
    def _create_racer_table(self, source_df, target_df):
        racer_df = pd.DataFrame()
        test = self._create_racer_array(target_df)
        source = self._create_racer_array(source_df)

        for racer_id in test:
            new_df = test[racer_id].copy()
            # 過去データ参照するとき用に順位に文字が入っているのを飛ばす (飛ばすのか変換するのか悩みどころ)
            source_racer = source[int(racer_id)].copy()
            source_racer['tmp'] = source_racer['着順'].map(lambda x: type(x) == int)
            source_racer = source_racer[source_racer['tmp']==True].drop(['tmp'], axis=1)
            source_racer['着順'] = source_racer['着順'].astype(int)

            for i, row in new_df.iterrows():
                # 着順を計算するラムダ
                calc_top1_proba = lambda df: 0 if len(df) == 0 else len(df[df['着順'] == 1])/len(df)
                calc_top3_proba = lambda df: 0 if len(df) == 0 else len(df[df['着順'] <= 3])/len(df)

                # 対象となるレース以前の日付のみのデータに
                source_prev = source_racer[source_racer['日付'] < row['日付']]
                stadium_code = row['スタジアムコード']
                teiban = row['艇番']

                # 全データ/会場/艇番 で絞ったその選手の過去データ
                all_df     = source_prev
                stadium_df = source_prev[source_prev['スタジアムコード'] == stadium_code]
                teiban_df  = source_prev[source_prev['艇番'] == teiban]

                # 全データの1着率,3着率の計算
                all_top1    = calc_top1_proba(all_df)
                all_top1_10 = calc_top1_proba(all_df.tail(10))
                all_top3    = calc_top3_proba(all_df)
                all_top3_10 = calc_top3_proba(all_df.tail(10))
                # スタジアムのデータの1着率,3着率の計算
                stadium_top1    = calc_top1_proba(stadium_df)
                stadium_top1_10 = calc_top1_proba(stadium_df.tail(10))
                stadium_top3    = calc_top3_proba(stadium_df)
                stadium_top3_10 = calc_top3_proba(stadium_df.tail(10))
                # コース別のデータの1着率,3着率の計算
                teiban_top1    = calc_top1_proba(teiban_df)
                teiban_top1_10 = calc_top1_proba(teiban_df.tail(10))
                teiban_top3    = calc_top3_proba(teiban_df)
                teiban_top3_10 = calc_top3_proba(teiban_df.tail(10))

                new_df.at[i, '全国_1着率_all'] = all_top1
                new_df.at[i, '全国_1着率_10'] = all_top1_10
                new_df.at[i, '全国_3着率_all'] = all_top3
                new_df.at[i, '全国_3着率_10'] = all_top3_10

                new_df.at[i, '当地_1着率_all'] = stadium_top1 
                new_df.at[i, '当地_1着率_10'] = stadium_top1_10 
                new_df.at[i, '当地_3着率_all'] = stadium_top3
                new_df.at[i, '当地_3着率_10'] = stadium_top3_10 

                new_df.at[i, 'コース別_1着率_all'] = teiban_top1 
                new_df.at[i, 'コース別_1着率_10'] = teiban_top1_10 
                new_df.at[i, 'コース別_3着率_all'] = teiban_top3
                new_df.at[i, 'コース別_3着率_10'] = teiban_top3_10 

            racer_df = pd.concat([racer_df, new_df])

        return racer_df

    
    
# pd.set_option('display.max_rows', 60)