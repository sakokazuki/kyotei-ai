# cf: https://teimon.jp/colab_de_go_3/

import os
import sys
from time import sleep
from requests import get
from datetime import datetime as dt
from datetime import timedelta as td
from os import makedirs
import lhafile
import re
import codecs

# lzhファイルの保存先を指定　
RACE_RESULTS_SAVE_DIR = os.path.join(os.getcwd(), 'results_lzh/')
BANGUMI_SAVE_DIR = os.path.join(os.getcwd(), 'results_bangumi_lzh/')
# lzhを解凍したファイルの保存先を指定
RACE_RESULTS_TXT_FILE_DIR = os.path.join(os.getcwd(), 'results_txt/')
BANGUMI_TXT_FILE_DIR = os.path.join(os.getcwd(), 'results_bangumi_txt/')


# リクエスト間隔を指定(秒)　※サーバに負荷をかけないよう3秒以上を推奨
INTERVAL = 3
# URLの固定部分を指定
race_results_url = "http://www1.mbrace.or.jp/od2/K/"
bangumi_url = "http://www1.mbrace.or.jp/od2/B/"


# レース結果をリクエストしてダウンロードする関数
def download_race_results(start_date, end_date):
    # ファイルを格納するフォルダを作成
    makedirs(RACE_RESULTS_SAVE_DIR, exist_ok=True)

    # 日付の差から期間を計算
    days_num = (end_date - start_date).days + 1

    # 日付リストを格納する変数
    date_list = []

    # 日付リストを生成
    for i in range(days_num):
        # 開始日から日付を順に取得
        target_date = start_date + td(days=i)

        # 日付型を文字列に変換してリストに格納(YYYYMMDD)
        date_list.append(target_date.strftime("%Y%m%d"))

    # URL生成とダウンロード
    for date in date_list:

        # URL生成
        yyyymm = date[0:4] + date[4:6]
        yymmdd = date[2:4] + date[4:6] + date[6:8]

        variable_url = race_results_url + yyyymm + "/k" + yymmdd + ".lzh"
        file_name = "k" + yymmdd + ".lzh"

        file_exist = os.path.isfile(RACE_RESULTS_SAVE_DIR + file_name)

        if(file_exist):
            # print('すでにダウンロード済みなのでスキップしました')
            continue

        # ダウンロード
        r = get(variable_url)

        # 成功した場合
        if r.status_code == 200:
            f = open(RACE_RESULTS_SAVE_DIR + file_name, 'wb')
            f.write(r.content)
            f.close()
            print(variable_url + " をダウンロードしました")

        # 失敗した場合
        else:
            print(variable_url + " のダウンロードに失敗しました")

        # 指定した間隔をあける
        sleep(INTERVAL)

# 番組表をリクエストしてダウンロードする関数
def download_bangumi(start_date, end_date):
    
    # ファイルを保存するフォルダを作成
    makedirs(BANGUMI_SAVE_DIR, exist_ok=True)

    # 日付の差から期間を計算
    days_num = (end_date - start_date).days + 1

    # 日付リストを格納する変数を定義
    date_list = []

    # 期間から日付を順に取り出す
    for d in range(days_num):
        # 開始日からの日付に変換
        target_date = start_date + td(days=d)

        # 日付(型)を文字列に変換してリストに格納(YYYYMMDD)
        date_list.append(target_date.strftime("%Y%m%d"))

    # 日付リストから日付を順に取り出す
    for date in date_list:

        # URL用に日付の文字列を生成
        yyyymm = date[0:4] + date[4:6]
        yymmdd = date[2:4] + date[4:6] + date[6:8]

        # URLとファイル名を生成
        variable_url = bangumi_url + yyyymm + "/b" + yymmdd + ".lzh"
        file_name = "b" + yymmdd + ".lzh"

        file_exist = os.path.isfile(BANGUMI_SAVE_DIR + file_name)

        if(file_exist):
            # print('すでにダウンロード済みなのでスキップしました')
            continue

        # 生成したURLでファイルをダウンロード
        r = get(variable_url)

        # 成功した場合
        if r.status_code == 200:
            # ファイル名を指定して保存
            f = open(BANGUMI_SAVE_DIR + file_name, 'wb')
            f.write(r.content)
            f.close()
            print(variable_url + " をダウンロードしました")

        # 失敗した場合
        else:
            print(variable_url + " のダウンロードに失敗しました")

        # 指定した間隔をあける
        sleep(INTERVAL)
        
# レース結果のlzhを解凍する関数
def decomp_race_results_lzh():
    # ファイルを格納するフォルダを作成
    os.makedirs(RACE_RESULTS_TXT_FILE_DIR, exist_ok=True)

    # LZHファイルのリストを取得
    lzh_file_list = os.listdir(RACE_RESULTS_SAVE_DIR)

    # ファイルの数だけ処理を繰り返す
    for lzh_file_name in lzh_file_list:


        # 拡張子が lzh のファイルに対してのみ実行
        if re.search(".lzh", lzh_file_name):

            file = lhafile.Lhafile(RACE_RESULTS_SAVE_DIR + lzh_file_name)

            # 解凍したファイルの名前を取得
            info = file.infolist()
            name = info[0].filename


            # 解凍したファイルの保存
            tmp_file_path = RACE_RESULTS_TXT_FILE_DIR + '_' + name
            file_path = RACE_RESULTS_TXT_FILE_DIR + name

            # 変換済みのファイルがある場合処理をスキップ
            file_exist = os.path.isfile(file_path)
            if(file_exist):
                # print('すでに解凍済みなのでスキップしました')
                continue

            # 一度一時ファイルに書き出す
            open(tmp_file_path, "wb").write(file.read(name))

            # 一時ファイルに書き出したものを開いてutf-8にエンコードする
            fin = open(tmp_file_path, "r")
            fout_utf = codecs.open(file_path, "w", "utf-8")
            for row in fin:
                fout_utf.write(row)
            fin.close()
            fout_utf.close()

            # 一時ファイルの削除
            os.remove(tmp_file_path)



            print(RACE_RESULTS_TXT_FILE_DIR + lzh_file_name + " を解凍しました")



def decomp_bangumi_lzh():
    # ファイルを格納するフォルダを作成
    os.makedirs(BANGUMI_TXT_FILE_DIR, exist_ok=True)

    # LZHファイルのリストを取得
    lzh_file_list = os.listdir(BANGUMI_SAVE_DIR)

    # ファイルの数だけ処理を繰り返す
    for lzh_file_name in lzh_file_list:


        # 拡張子が lzh のファイルに対してのみ実行
        if re.search(".lzh", lzh_file_name):

            file = lhafile.Lhafile(BANGUMI_SAVE_DIR + lzh_file_name)

            # 解凍したファイルの名前を取得
            info = file.infolist()
            name = info[0].filename


            # 解凍したファイルの保存
            tmp_file_path = BANGUMI_TXT_FILE_DIR + '_' + name
            file_path = BANGUMI_TXT_FILE_DIR + name

            # 変換済みのファイルがある場合処理をスキップ
            file_exist = os.path.isfile(file_path)
            if(file_exist):
                # print('すでにダウンロード済みなのでスキップしました')
                continue

            # 一度一時ファイルに書き出す
            open(tmp_file_path, "wb").write(file.read(name))

            # 一時ファイルに書き出したものを開いてutf-8にエンコードする
            fin = open(tmp_file_path, "r")
            fout_utf = codecs.open(file_path, "w", "utf-8")
            for row in fin:
                fout_utf.write(row)
            fin.close()
            fout_utf.close()

            # 一時ファイルの削除
            os.remove(tmp_file_path)

            print(BANGUMI_TXT_FILE_DIR + lzh_file_name + " を解凍しました")

        
def download_race_data(start_date, end_date):
    download_race_results(start_date, end_date)
    download_bangumi(start_date, end_date)
    decomp_race_results_lzh()
    decomp_bangumi_lzh()
    

    

# ファイル数が一致するか確かめる
def test_file_count():
    race_results_file_count = sum(os.path.isfile(os.path.join(RACE_RESULTS_TXT_FILE_DIR, name)) for name in os.listdir(RACE_RESULTS_TXT_FILE_DIR))

    bangumi_file_count = sum(os.path.isfile(os.path.join(BANGUMI_TXT_FILE_DIR, name)) for name in os.listdir(BANGUMI_TXT_FILE_DIR))
    
    return bangumi_file_count == race_results_file_count 
    
# テスト実行
def test():
    
    if not test_file_count():
        print("レース結果と番組表ファイルの数が合いません")
        return
        
    print("download_race_data: テスト成功")
    
        
    
    
    
    
    
    