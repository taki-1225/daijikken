import pandas as pd
import datetime
import glob
from pandas import DataFrame

FILE_PATH = '201911~201912/'                        #fileへのパス(相対)
WRITE_PATH = FILE_PATH.replace('/', '_extract/')    #書き込み先のディレクトリ、前もって作っておく必要がある、名前は'元のファイル名+_extract'
PATH_LEN = len(FILE_PATH)

AMPID_RATE = 0.5                                    #AMPIDの観測割合による削除基準
INTERVAL = 1.25                                     #観測間隔による削除基準

start_time = datetime.datetime.now()
#######################################################################################################
    ##時刻の形式が 0000/00/00 00:00.00である必要あり。##
#######################################################################################################
def extract_random_closed(df, flag):
    unique_list = ['0','1','4','5','8','9','C','D']          #ランダム排除用配列
    extraction_list = []                                    #抽出する行を格納
    row_count = 0                                           #要素数をカウント
    if flag == 0:
        check_list = ['0', '1', '2', '3', '4', '5', '6']      #抽出時刻リスト
    elif flag == 1:
        check_list = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
    
    for i ,j in zip(df['OUI'],df['TIMESTAMP']):             #同時に2つの要素を扱う zip関数はそんな感じ
        if i[1] in unique_list:                              #ユニークを抽出
            if j[11] == '0' and j[12] == '9':               #09を抽出
                extraction_list.append(row_count)
            elif j[11] == '1' and j[12] in check_list:      #10:00~1700を抽出
                extraction_list.append(row_count)
            elif flag == 1 and j[11:13] == '20' and int(j[14:16]) <= 30:
                extraction_list.append(row_count)
        row_count += 1

    df = df.query('index in @extraction_list')              #抽出行を取り出す
    df = df.sort_values(['AMAC', 'UNIXTIME']) #まずはソート。めっちゃはやい。
    df = df.reset_index(drop=True)       #インデックス番号を振り直してる　なくてもいいかも
    return df


def drop_non_visitor(df):
    df['STAY TIME'] = ''    #STAY TIME列を追加
    drop_list = []          #削除AMACのlist
    dict_AMPID = {}         #AMPIDのカウント回数を記録する辞書
    mac = ''                #MAC
    visitor = 0             #来園者数
    vt = 0             #visit time: 来園時間
    lt = 0             #leave time: 退園時間
    st = 0             #stay time: 滞在時間 
    flag = 0           #観測間隔のフラグ（2時間以上間隔が空いたときにflag = 1)
    count = 0          #同一MACの観測回数
    index = 0          #indexのカウント
    for row in df.itertuples():                     #上から1行ずつ見ていく
        if mac != row.AMAC:                         #初めてのMACのとき  
            st = lt - vt                     
            if st < 30*60 or count < 7 or flag == 1:             #滞在時間が30分未満か観測回数が7回未満で削除
                drop_list.append(mac)
                flag = 0                                         #flagをリセット
            elif max(dict_AMPID.values()) / count > AMPID_RATE:         #AMPID観測数の最大値の割合が6割を超えたとき
                drop_list.append(mac)                            #削除
            else:
                visitor += 1                                     #visitorカウント
                df.at[index-1, 'STAY TIME'] = st                 #滞在時間を追加
            #以下、変数の初期化・更新
            mac = row.AMAC
            dict_AMPID = {}
            dict_AMPID[row.AMPID] = 1
            vt = int(row.UNIXTIME)
            lt = int(row.UNIXTIME)
            count = 1
        elif mac == row.AMAC and flag == 0:
            if int(row.UNIXTIME) - lt > INTERVAL*60*60:    #観測間隔が2時間以上空くと削除
                flag = 1
            else:
                AMPID_process(dict_AMPID, row)      #AMPIDをカウントする関数
                lt = int(row.UNIXTIME)
                count += 1
        index += 1
     
    st = lt - vt                     
    if st < 30*60 or count < 7 or flag == 1:             #滞在時間が30分未満か観測回数が7回未満で削除
        drop_list.append(mac)
    elif max(dict_AMPID.values()) / count > 0.6:         #AMPID観測数の最大値の割合が6割を超えたとき
        drop_list.append(mac)                            #削除
    else:
        visitor += 1                                     #visitorカウント
        df.at[index, 'STAY TIME'] = st                 #滞在時間を追加

    #df = df[~df['AMAC'].isin(drop_list)]           #listのMACを削除
    df = df.query('AMAC not in @drop_list')         #query使ってMAC削除(こっちのが早いらしい)
    df = df.reset_index(drop=True)                  #インデックス番号を振り直してる　なくてもいいかも
    print('Number of visitor:', visitor)
    df.at[0, 'Number of visitor'] = visitor         #visitorの列を追加
    return df


def AMPID_process(dict_AMPID, row):             #AMAC毎にAMPIDをカウントする関数
    if row.AMPID not in dict_AMPID:             #初めてのAMPIDに出会ったとき
        dict_AMPID[row.AMPID] = 1               #辞書に追加
    else:                                       #すでに登録されているAMPIDについて
        dict_AMPID[row.AMPID] +=1               #カウントを増やす



def file_process(filename, flag):
    df = pd.read_csv(filename)
    df = extract_random_closed(df, flag)
    df = drop_non_visitor(df)
    df.to_csv(WRITE_PATH+filename[PATH_LEN:PATH_LEN+8]+".csv")    #csvファイル出力


load_wild = FILE_PATH + '*.csv'
file_list = glob.glob(load_wild)
night_open = ['20191115', '20191116', '20191117', '20191124', '20191130', '20191201']
count = 0
for filename in file_list:
    start_time1 = datetime.datetime.now()    #計測開始
    if filename[PATH_LEN:PATH_LEN+8] not in night_open:
        file_process(filename, 0)
    elif filename[PATH_LEN:PATH_LEN+8] in night_open:
        file_process(filename, 1)
    count += 1
    end_time1 = datetime.datetime.now()    #計測終了
    print(count,"files complete : ", filename[PATH_LEN:PATH_LEN+8], '\n'
        "process time : ",end_time1 - start_time1)
    
end_time = datetime.datetime.now()    #計測終了
print("process time : ",end_time-start_time)


