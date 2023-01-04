
# レース結果と返金をデータフレームに
import os
import re
import datetime as dt
import numpy as np
import pandas as pd
import utils
from tqdm import tqdm

# レース結果テキストのパース
def parse_race_result_txet_file(text_file):
    retval_race  = []
    retval_return = []
    for line in text_file:
        if re.search(r"競走成績", line):
            # 1行スキップ
            text_file.readline()
            
            # タイトルを格納 
            # ------------------------------------------------------------------------------------
            line = text_file.readline()
            title = line[:-1].strip()
            # 1行スキップ
            text_file.readline()
            
            # 日次・レース日・レース場を格納
            # ------------------------------------------------------------------------------------
            line = text_file.readline()
            day = line[3:7].replace(' ', '')
            date = line[17:27].replace(' ', '0')
            stadium = line[62:65].replace('　', '')
            # 1行スキップ
            text_file.readline()
            
        # レース回の「R」と距離の「H」を同じ行に見つけたら -> これ以降に競走成績の詳細が記載
        # ex)    3R       一般　　　　                 H1800m  晴　  風  北東　 2m  波　  3cm
        if re.search(r"R", line) and re.search(r"H", line):
            # レース回、レース名、距離(m)、天候、風向、風速(m)、波の高さ(cm)を取得
            # ------------------------------------------------------------------------------------
            
            # レース名にキーワード「進入固定」が割り込んだ際の補正(「進入固定戦隊」は除くためＨまで含めて置換)
            # ex)    8R       予選　　　　  進入固定       H1800m  曇り  風  北東　 3m  波　  3cm
            if re.search(r"進入固定", line):
                line = line.replace('進入固定       H', '進入固定           H')
                
            race_round = line[2:5].replace(' ', '0')
            race_name = line[12:31].replace('　', '')
            distance = line[36:40]
            weather = line[43:45].strip()
            wind_direction = line[50:52].strip()
            wind_velocity = line[53:55].strip()
            wave_height = line[60:63].strip()

            # 決まり手を取得
            line = text_file.readline()
            winning_technique = line[50:55].strip()

            # 1行スキップ
            text_file.readline()
            
            # 選手データの取得
            # 01  1 4424 松　尾　　昂　明 22  154  6.84   1    0.18     1.50.6
            # 02  5 4930 佐　藤　　　　悠 58  138  6.80   5    0.29     1.51.7
            # 03  2 3614 谷　　　　勝　幸 55  147  6.87   2    0.27     1.53.5
            # 04  4 3587 武　田　　信　一 53  170  6.90   4    0.24     1.55.8
            # 05  6 5155 内　山　　七　海 65  156  6.85   6    0.27      .  . 
            # 06  3 4346 前　田　　健太郎 72  123  6.95   3    0.18      .  . 
            # ------------------------------------------------------------------------------------
            # 選手データを格納する変数を定義
            result_racer = []

            # 選手データを取り出す行(開始行)を格納
            line = text_file.readline()

            # 空行まで処理を繰り返す = 1～6艇分の選手データを取得
            while line != "\n":
                # 選手データを格納(行末にカンマが入らないように先頭にカンマを入れる)
                result_racer.append(line[2:4] + "," + line[6] + "," + line[8:12] \
                                + "," + line[13:21] + "," + line[22:24] + "," + line[27:29] \
                                + "," + line[30:35].strip() + "," + line[38] + "," + line[43:47] \
                                + "," + line[52:58])

                # 次の行を読み込む
                line = text_file.readline()
             
            # レース結果を取り出す行(開始行)を格納
            line = text_file.readline()

            # 空行まで処理を繰り返す = レース結果を取得
            race_success = True
            while line != "\n":
                # 原因はわからないが不成立なゲームがある
                if re.search(r"レース不成立", line):
                    race_success = False

                # 単勝の結果を取得
                # 単勝     1          100  
                # 単勝        特払い   70 
                # --------------------------------------------------------------------------------
                if re.search(r"単勝", line):

                    # 文字列「特払い」が割り込んだ際の補正
                    if re.search(r"特払い", line):
                        line = line.replace('        特払い   ', '   特払い        ')

                    result_win = line[15] + "," + line[22:29].strip()

                # 複勝の結果を取得
                # 複勝     1          110  5          100
                # --------------------------------------------------------------------------------
                if re.search(r"複勝", line):

                    # 文字列「特払い」が割り込んだ際の補正
                    if re.search(r"特払い", line):
                        line = line.replace('        特払い   ', '   特払い        ')

                    # 複勝_2着のデータが存在しない場合の分岐
                    if len(line) <= 33:
                        result_place_show = line[15] + "," + line[22:29].strip() \
                                            + "," + ","
                    else:
                        result_place_show = line[15] + "," + line[22:29].strip() \
                                            + "," + line[31].strip() + "," + line[38:45].strip()

                # 2連単の結果を取得
                # ２連単   1-5        450  人気     3 
                # --------------------------------------------------------------------------------
                if re.search(r"２連単", line):
                    result_exacta = line[14:17] + "," + line[21:28].strip() \
                                    + "," + line[36:38].strip()

                # 2連複の結果を取得
                # ２連複   1-5        290  人気     1
                # --------------------------------------------------------------------------------
                if re.search(r"２連複", line):
                    # 文字列「特払い」が割り込んだ際の補正
                    # ２連複   1-3        170  人気     1 
                    # ２連複      特払い   70
                    if re.search(r"特払い", line):
                        line = line.replace('      特払い   ', ' 特払い        ')
                        
                    result_quinella = line[14:17].strip() + "," + line[21:28].strip() \
                                      + "," + line[36:38].strip()

                # 拡連複の結果を取得
                # 拡連複   1-5        120  人気     1 
                #          1-2        140  人気     2 
                #          2-5        510  人気    11 
                # --------------------------------------------------------------------------------
                if re.search(r"拡連複", line):
                    # 1-2着
                    result_quinella_place = line[14:17] + "," + line[21:28].strip() \
                                            + "," + line[36:38].strip()

                    # 1-3着
                    line = text_file.readline()
                    result_quinella_place += "," + line[17:20] + "," + line[24:31].strip() \
                                             + "," + line[39:41].strip()

                    # 2-3着
                    line = text_file.readline()
                    result_quinella_place += "," + line[17:20] + "," + line[24:31].strip() \
                                             + "," + line[39:41].strip()

                # 3連単の結果を取得
                # ３連単   1-5-2     1250  人気     4 
                # --------------------------------------------------------------------------------
                if re.search(r"３連単", line):
                    result_trifecta = line[14:19] + "," + line[21:28].strip() \
                                      + "," + line[35:38].strip()

                # 3連複の結果を取得
                # ３連複   1-2-5      280  人気     1 
                # --------------------------------------------------------------------------------
                if re.search(r"３連複", line):
                    result_trio = line[14:19] + "," + line[21:28].strip() \
                                  + "," + line[35:38].strip()

                # 次の行を読み込む
                line = text_file.readline()
                
            dt_date = dt.datetime.strptime(date, '%Y/%m/%d')
            race_code = utils.create_race_code(dt_date, stadium, race_round[0:2])
            dt_date = dt_date + dt.timedelta(hours=int(race_round[0:2]))
            
            # 不成立なレースをスキップ
            if race_success == False:
                continue
                
            # print(race_code + "," + title + "," + day + "," + date + "," + stadium \
            #                + "," + race_round + "," + race_name + "," + distance + "," + weather \
            #                + "," + wind_direction + "," + wind_velocity + "," + wave_height \
            #                + "," + winning_technique + "," + result_win + "," + result_place_show \
            #                + "," + result_exacta + "," + result_quinella + "," + result_quinella_place \
            #                + "," + result_trifecta + "," + result_trio + result_racer + "\n")
            
            order = 1
            
            # レーサーの結果を作成
            for data_txt in result_racer:
                data = data_txt.split(',')
                
                row = {}
                row['レースコード'] = race_code
                row['スタジアムコード'] = utils.kanji_to_stadium_code(stadium)
                row['日付'] = dt_date
                # print(data)
                is_ketujo = re.findall('K', data[0])
                is_late = re.findall('L', data[0])
                # print(re.findall('S|K|F|K', data[0]), data[0])
                # F フライング
                # L0 選手責任外の出遅れ
                # L1 選手責任の出遅れ
                # K0 選手責任外の事前欠場
                # K1 選手責任の事前欠場
                # S0 選手責任外の失格
                # S1 選手責任の失格
                # S2 他艇を妨害・失格
                if re.findall('S|K|F|K|L', data[0]):
                    row['着順'] = data[0]
                else:
                    row['着順'] = int(data[0])
                    
                    
                row['艇番'] = int(data[1])
                row['登録番号'] = int(data[2])
                row['選手名'] =  ''.join(data[3].split())
                row['モーター番号'] = int(data[4])
                row['ボート番号'] = int(data[5])
                if is_ketujo:
                    row['展示タイム'] = np.nan
                else:
                    row['展示タイム'] = float(data[6])
                # datetime.time型にする？
                # row['展示タイム'] = dt.time(0,0,int(data[6].split('.')[0]), int(data[6].split('.')[1]))

                if is_ketujo or is_late:
                    row['進入コース'] = np.nan
                else:
                    row['進入コース'] = int(data[7])
                                       
                if is_ketujo or is_late:
                    row['スタートタイミング'] = np.nan
                else:
                    row['スタートタイミング'] = float(data[8])
                
                # 1.51.1を秒数に変換
                race_time = data[9].split('.')
                if is_ketujo or is_late or race_time[0] == ' ' :
                    row['レースタイム'] = np.nan
                else:
                    row['レースタイム'] = int(race_time[0]) * 60 + int(race_time[1]) + int(race_time[0]) * 0.1
                row['距離(m)'] = int(distance)
                row['天候'] = weather
                row['風向'] = wind_direction
                row['風速(m)'] = int(wind_velocity)
                row['波の高さ(cm)'] = int(wave_height)
                row['決まり手'] = winning_technique
                
                retval_race.append(row)
                order+=1
                
            # 配当リストの作成
            row = {}
            to_int = lambda x : int(x)
            to_float = lambda x : float(x)
            to_int_array = lambda x : [int(i) for i in x.split('-')]
            valid_i_f = lambda x, func: x if re.findall('い|不', x) else func(x)
            valid_empty = lambda x, func: x if x == '' else func(x)
            
            to_int_array = lambda x, : x if re.findall('い|不', x) else [int(i) for i in x.split('-')]
            s = result_win.split(',')
            row['レースコード'] = race_code
            row['日付'] = dt_date
            row['単勝_艇番'] = s[0] 
            row['単勝_払戻金'] = s[1] 
            
            s = result_place_show.split(',')
            row['複勝_1着_艇番'] = s[0] 
            row['複勝_1着_払戻金'] = s[1]
            row['複勝_2着_艇番'] = s[2] 
            row['複勝_2着_払戻金'] = s[3] 
            
            # 2連単
            s = result_exacta.split(',')
            row['2連単_艇番'] = s[0]
            row['2連単_払戻金'] = s[1]
            
            # 2連複
            s = result_quinella.split(',')
            row['2連複_艇番'] = s[0]
            row['2連複_払戻金'] = s[1]
            
            # 拡連複
            s = result_quinella_place.split(',')
            row['拡連複_1_艇番'] = s[0]
            row['拡連複_1_払戻金'] = s[1]
            row['拡連複_2_艇番'] = s[3]
            row['拡連複_2_払戻金'] = s[4]
            row['拡連複_3_艇番'] = s[6]
            row['拡連複_3_払戻金'] = s[7]
                
            # 3連単
            s = result_trifecta.split(',')
            row['3連単_艇番'] = s[0]
            row['3連単_払戻金'] = s[1]
            
            # 3連複
            s = result_trio.split(',')
            row['3連複_艇番'] = s[0]
            row['3連複_払戻金'] = s[1]
            
            is_str = lambda v: type(v) is str
            is_list = lambda v: type(v) is list

            for key in row:
                value = row[key]
                if key == 'レースコード' :
                    continue
                    
                # 艇番の数字以外が入ったときの処理
                if re.findall('艇番', key):
                    # その他連複、連単系は配列をintに
                    if is_list(value) :
                        
                        # 複勝に特払いが入ることがあるので-2に
                        if re.findall('い', value[0]):
                            row[key] = [-1]
                            
                        # 複勝に不成立が入ることがあるので-2に
                        if re.findall('不', value[0]):
                            row[key] = [-2]
                            
                        # 拡連複1が不成立の場合、2以降が''になる
                        if value[0] == '':
                            row[key] = [-2]
                            
                        row[key] = [int(i) for i in row[key]]
                    # 単勝、複勝の数字をint型に
                    else:
                        # 艇番の'い'は特払いで-1にする
                        if re.findall('い', value):
                            row[key] = -1
                        
                        # 複勝に''が入ることが有り、無投票0とする
                        if value == '':
                            row[key] = 0
                            
                        # 複勝に'不'が入ることが有り、不成立-2とする
                        if re.findall('不', value):
                            row[key] = -2
                        
                        if value == ' ':
                            print(row)
                            
                        # 2連以上は2-2という表記のためint変換をスキップ
                        if len(value.split('-')) == 1:
                            row[key] = int(row[key])
                        
                
                if re.findall('払戻金', key):
                    if value == '':
                        row[key] = 100
                    row[key] = int(row[key])
            
            retval_return.append(row)
            
    return retval_race, retval_return

# レーステーブルと返金テーブルの作成
def create_race_table(start_date='', end_date=''):    
    result_array = []
    return_array = []

    # 解凍したテキストファイルの格納先を指定
    TEXT_FILE_DIR = os.path.join(os.getcwd(), 'results_txt/')

    # テキストファイルのリストを取得
    text_file_list = os.listdir(TEXT_FILE_DIR)

    # リストからファイル名を順に取り出す
    for text_file_name in text_file_list:
        
        # file_name_date = dt.datetime.strptime(, '%Y/%m/%d')


        # 拡張子が TXT のファイルに対してのみ実行
        if re.search(".TXT", text_file_name):
            
            file_name_date_str = '20' + text_file_name[1:7]
            file_name_date = dt.datetime.strptime(file_name_date_str, '%Y%m%d')
            
            is_skip = False
            if start_date != '' and end_date != '':
                start = dt.datetime.strptime(start_date, '%Y-%m-%d')
                end   = dt.datetime.strptime(end_date,   '%Y-%m-%d')
                is_skip = not (start <= file_name_date and file_name_date <= end)
            elif start_date != '':
                start = dt.datetime.strptime(start_date, '%Y-%m-%d')
                is_skip = not (start <= file_name_date) 
            elif end_date != '':
                end   = dt.datetime.strptime(end_date,   '%Y-%m-%d')
                is_skip = not (file_name_date <= end)
                
            if is_skip:
                continue
            
                
            text_file = open(TEXT_FILE_DIR + text_file_name, "r", encoding="utf_8")
            _race, _return = parse_race_result_txet_file(text_file)
            result_array.extend(_race)
            return_array.extend(_return)
            text_file.close()

    df_race = pd.DataFrame(result_array)
    df_return = pd.DataFrame(return_array)
    return df_race, df_return



# 番組表をデータフレームに
def parse_bangumi_txet_file(text_file):
    retval = []
    
    # テキストファイルから中身を順に取り出す
    for contents in text_file:

        trans_asc = str.maketrans('１２３４５６７８９０Ｒ：　', '1234567890R: ')

        # キーワード「番組表」を見つけたら(rは正規表現でraw文字列を指定するおまじない)
        if re.search(r"番組表", contents):
            # 1行スキップ
            text_file.readline()

            # タイトルを格納
            line = text_file.readline()
            title = line[:-1].strip()

            # 1行スキップ
            text_file.readline()

            # 日次・レース日・レース場を格納
            line = text_file.readline()
            day = line[3:7].translate(trans_asc).replace(' ', '')
            date = line[17:28].translate(trans_asc).replace(' ', '0')
            stadium = line[52:55].replace('　', '')

        # キーワード「電話投票締切予定」を見つけたら
        if re.search(r"電話投票締切予定", contents):

            # キーワードを見つけた行を格納
            line = contents

            # レース名にキーワード「進入固定」が割り込んだ際の補正(「進入固定戦隊」は除くためＨまで含めて置換)
            if re.search(r"進入固定", line):
                line = line.replace('進入固定 Ｈ', '進入固定     Ｈ')

            # レース回・レース名・距離(m)・電話投票締切予定を格納
            race_round = line[0:3].translate(trans_asc).replace(' ', '0')
            race_name = line[5:21].replace('　', '')
            distance = line[22:26].translate(trans_asc)
            post_time = line[37:42].translate(trans_asc)

            # 4行スキップ(ヘッダー部分)
            text_file.readline()
            text_file.readline()
            text_file.readline()
            text_file.readline()

            # 選手データを格納する変数を定義
            racer_data = []

            # 選手データを読み込む行(開始行)を格納
            line = text_file.readline()

            # 空行またはキーワード「END」まで処理を繰り返す = 1～6艇分の選手データを取得
            while line != "\n":

                if re.search(r"END", line):
                    break

                # 選手データを格納(行末にカンマが入らないように先頭にカンマを入れる)
                racer_data.append(line[0] + "," + line[2:6] + "," + line[6:10] + "," + line[10:12] \
                              + "," + line[12:14] + "," + line[14:16] + "," + line[16:18] \
                              + "," + line[19:23] + "," + line[24:29] + "," + line[30:34] \
                              + "," + line[35:40] + "," + line[41:43] + "," + line[44:49] \
                              + "," + line[50:52] + "," + line[53:58] + "," + line[59:60] \
                              + "," + line[60:61] + "," + line[61:62] + "," + line[62:63] \
                              + "," + line[63:64] + "," + line[64:65] + "," + line[65:66] \
                              + "," + line[66:67] + "," + line[67:68] + "," + line[68:69] \
                              + "," + line[69:70] + "," + line[70:71] + "," + line[71:73])

                # 次の行を読み込む
                line = text_file.readline()

                
            # レースコードを生成
            dt_date = dt.datetime.strptime(date, '%Y年%m月%d日')
            race_code = utils.create_race_code(dt_date, stadium, race_round[0:2])
            
            for data_txt in racer_data:
                data = data_txt.split(',')
                
                # F フライング
                # L0 選手責任外の出遅れ
                # L1 選手責任の出遅れ
                # K0 選手責任外の事前欠場
                # K1 選手責任の事前欠場
                # S0 選手責任外の失格
                # S1 選手責任の失格
                # S2 他艇を妨害・失格
                valid_space = lambda x: np.nan if re.findall(' |S|F|K|L', x) else int(x)
                
                row = {}
                # print(data)
                row['レースコード'] = race_code
                row['艇番'] = int(data[0])
                row['登録番号'] = int(data[1])
                row['選手名'] = data[2].strip()
                row['年齢'] = int(data[3])
                row['支部'] = data[4]
                row['体重'] = int(data[5])
                row['級別'] = data[6]
                row['全国勝率'] = float(data[7])
                row['全国2連対率'] = float(data[8])
                row['当地勝率'] = float(data[9])
                row['当地2連対率'] = float(data[10])
                row['モーター番号'] = int(data[11])
                row['モーター2連対率'] = float(data[12])
                row['ボート番号'] = int(data[13])
                row['ボート2連対率'] = float(data[14])
                row['今節成績_1-1'] = valid_space(data[15])
                row['今節成績_1-2'] = valid_space(data[16])
                row['今節成績_2-1'] = valid_space(data[17])
                row['今節成績_2-2'] = valid_space(data[18])
                row['今節成績_3-1'] = valid_space(data[19])
                row['今節成績_3-2'] = valid_space(data[20])
                row['今節成績_4-1'] = valid_space(data[21])
                row['今節成績_4-2'] = valid_space(data[22])
                row['今節成績_5-1'] = valid_space(data[23])
                row['今節成績_5-2'] = valid_space(data[24])
                row['今節成績_6-1'] = valid_space(data[25])
                row['今節成績_6-2'] = valid_space(data[26])
                
                retval.append(row)

            # 抽出したデータをCSVファイルに書き込む
            # print(race_code + "," + title + "," + day + "," + date + "," + stadium + "," + race_round
            #                + "," + race_name + "," + distance + "," + post_time + racer_data + "\n")
            
    return retval 
    


# 番組テーブルの作成
def create_bangumi_table(start_date='', end_date=''):
    bangumi_array = []

    # 解凍したテキストファイルの格納先を指定
    TEXT_FILE_DIR = os.path.join(os.getcwd(), 'results_bangumi_txt/')

    # テキストファイルのリストを取得
    text_file_list = os.listdir(TEXT_FILE_DIR)

    # リストからファイル名を順に取り出す
    for text_file_name in text_file_list:

        # 拡張子が TXT のファイルに対してのみ実行
        if re.search(".TXT", text_file_name):
            
            file_name_date_str = '20' + text_file_name[1:7]
            file_name_date = dt.datetime.strptime(file_name_date_str, '%Y%m%d')
            
            is_skip = False
            if start_date != '' and end_date != '':
                start = dt.datetime.strptime(start_date, '%Y-%m-%d')
                end   = dt.datetime.strptime(end_date,   '%Y-%m-%d')
                is_skip = not (start <= file_name_date and file_name_date <= end)
            elif start_date != '':
                start = dt.datetime.strptime(start_date, '%Y-%m-%d')
                is_skip = not (start <= file_name_date) 
            elif end_date != '':
                end   = dt.datetime.strptime(end_date,   '%Y-%m-%d')
                is_skip = not (file_name_date <= end)
                
            if is_skip:
                continue
            
            text_file = open(TEXT_FILE_DIR + text_file_name, "r", encoding="utf_8")
            _bangumi = parse_bangumi_txet_file(text_file)
            bangumi_array.extend(_bangumi)
            text_file.close()
        

    df_bangumi = pd.DataFrame(bangumi_array)
    return df_bangumi

# レーステーブルをレーサーごとに分けるj
def create_racer_array(result_table):
    li = {}
    for no in result_table['登録番号'].unique():
        li[no] = result_table[result_table['登録番号'] == no]
    return li

def create_racer_table(source_df, past_data, need_update=False):
    racer_df = pd.DataFrame()
    target = source_df

    # 既存のテーブルを使う場合
    if need_update == False:
        # pickleから復元
        racer_df = past_data
        # 既存のテーブルに含まれていないレースコードのデータのみ抽出
        target = source_df[~source_df["レースコード"].isin(racer_df['レースコード'])]

    test = create_racer_array(target) 
    source = create_racer_array(source_df)

    for racer_id in tqdm(test):
        new_df = test[racer_id].copy()
        # 過去データ参照するとき用に順位に文字が入っているのを飛ばす (飛ばすのか変換するのか悩みどころ)
        source_racer = source[racer_id].copy()
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

class DataTables:
    def __init__(self, need_update=False, update_all=False):
        
        # 全データをパースし直す
        if update_all == True:
            race_table, return_table = create_race_table()
            bangumi_table = create_bangumi_table()
            race_table.to_pickle('pic_race_table')
            return_table.to_pickle('pic_return_table')
            bangumi_table.to_pickle('pic_bangumi_table')
            
        # 追加データの更新
        elif need_update == True:
            race_table = pd.read_pickle('pic_race_table')
            return_table = pd.read_pickle('pic_return_table')
            bangumi_table = pd.read_pickle('pic_bangumi_table')
            
            # 保存済みのテーブルの最後の日にちを取得し、その次の日のstringを得る
            last_save_date = race_table.tail(1).loc[:, '日付']
            start_date = last_save_date - dt.timedelta(1)
            start_date_str = start_date.iloc[-1].strftime('%Y-%m-%d')
            
            # 新しく追加されたデータを拾う
            new_race_table, new_return_table = create_race_table(start_date=start_date_str)
            new_bangumi_table = create_bangumi_table(start_date=start_date_str)
            # 新しく追加されたデータのうち、まだテーブルに含まれていないものだけを抽出
            new_race_table = self._extract_non_dupulicate_race_id(race_table, new_race_table)
            new_return_table = self._extract_non_dupulicate_race_id(return_table, new_return_table)
            new_bangumi_table = self._extract_non_dupulicate_race_id(bangumi_table, new_bangumi_table)
            
            race_table = pd.concat([race_table, new_race_table])
            return_table = pd.concat([return_table, new_return_table])
            bangumi_table = pd.concat([bangumi_table, new_bangumi_table])
            
            race_table.to_pickle('pic_race_table')
            return_table.to_pickle('pic_return_table')
            bangumi_table.to_pickle('pic_bangumi_table')
        
        # 更新をせずpickleから読み込むだけ
        else:
            race_table = pd.read_pickle('pic_race_table')
            return_table = pd.read_pickle('pic_return_table')
            bangumi_table = pd.read_pickle('pic_bangumi_table')
            
        racer_table = create_racer_table(race_table, pd.read_pickle("pic_racer_table"), update_all)
        racer_table.to_pickle("pic_racer_table")
        
        self.race_t = race_table
        self.racer_t = racer_table
        self.return_t = return_table
        self.bangumi_t = bangumi_table
        self.merged_t = pd.merge(race_table, bangumi_table.drop(['モーター番号', 'ボート番号', '選手名', '登録番号'], axis=1), on=['レースコード', '艇番'])
        self.merge_racer_race()
    
    # レーステーブルにレーサごとの勝率データが入ったテーブルをマージ
    def merge_racer_race(self):
        racer_t = self.racer_t.drop(['スタジアムコード', '日付', '着順', '登録番号', '選手名', 'モーター番号', 'ボート番号', '展示タイム', '進入コース', 'スタートタイミング', 'レースタイム', '距離(m)', '天候', '風向', '風速(m)', '波の高さ(cm)', '決まり手'], axis=1)
        self.merged_t = pd.merge(self.merged_t, racer_t, on=['レースコード', '艇番'])
        
    # 出走データはあるけど結果がないrace_idを確認(キャンセルになったレース?)
    def extract_cancel_race_bangumi_table(self):
        return self._extract_non_dupulicate_race_id(self.race_t, self.bangumi_t)
        
    
    def _extract_non_dupulicate_race_id(self, src, target):
        unique = src['レースコード'].unique()
        # print(unique)
        # print(target[~target['レースコード'].isin(unique)])
        processed = target[~target['レースコード'].isin(unique)]
        return processed
    
    def test(self):
        
        if not self._test_same_row():
            print("レースの結果と返金のテーブルの数が合いません")
            return
        
        print('RaceTables:テスト成功')
              
    def _test_same_row(self):
        len_race = len(self.race_t)
        len_racer = len(self.racer_t)
        len_return = len(self.return_t)
        len_merged = len(self.merged_t)
        
        return len_race == len_return*6 and len_race == len_merged and len_race == len_racer