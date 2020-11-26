import pandas as pd
import datetime
from pandas import DataFrame

start_time = datetime.datetime.now()    #計測開始

df = pd.read_csv('201910~202010/20191105.csv')


#######################################################################################################
    ##時刻の形式が 0000/00/00 00:00.00である必要あり。##
#######################################################################################################
def drop_random_closed():
    global df
    check_list = ['0','1','4','5','8','9','C','D']          #ランダム排除用配列
    check_list_1 = ['0','1','2','3','4','5','6','7','8']    #時刻排除用
    check_list_2 = ['7','8','9']                            #時刻排除用その２
    random_list = []                                        #排除する行を格納
    row_count = 0                                           #要素数をカウント
    for i ,j in zip(df['OUI'],df['TIMESTAMP']): #同時に2つの要素を扱う zip関数はそんな感じ
        if i[1] in check_list:      #ユニークを抽出
            if j[11] == '0' and j[12] in check_list_1:      #00:~08:を排除
                random_list.append(row_count)
            if j[11] == '2':                                #20:~23:を排除
                random_list.append(row_count)
            if j[11] == '1' and j[12] in check_list_2:      #17:~19:を排除
                random_list.append(row_count)
        else:       #ランダムは排除
            random_list.append(row_count)
        row_count += 1

    df = df.drop(random_list)

    #滞在時間long_time分未満のMACアドレスを間引く
    df = df.sort_values(['AMAC', 'UNIXTIME']) #まずはソート。めっちゃはやい。
    df = df.reset_index(drop=True)       #インデックス番号を振り直してる　なくてもいいかも


def drop_non_visitor():
    global df
    df['STAY TIME'] = ''
    drop_list = []
    mac = ''           #MAC
    vt = 0             #visit time: 来園時間
    lt = 0             #leave time: 退園時間
    st = 0             #stay time: 滞在時間 
    count = 0          #同一MACの観測回数
    index = 0
    visitor = 0
    print(df)
    for row in df.itertuples():                     #上から1行ずつ見ていく
        if mac != row.AMAC:                         #初めてのMACのとき  
            st = lt - vt                     
            if st < 30*60 or count < 7:             #滞在時間が30分未満か観測回数が7回未満で削除
                drop_list.append(mac)
            else:
                df.at[index-1, 'STAY TIME'] = st
                visitor += 1
            mac = row.AMAC
            vt = int(row.UNIXTIME)
            lt = int(row.UNIXTIME)
            count = 1
        elif mac == row.AMAC:
            if int(row.UNIXTIME) - lt > 2*60*60:    #観測間隔が2時間以上空くと削除
                drop_list.append(mac)
            lt = int(row.UNIXTIME)
            count += 1
        index += 1

    print('Number of visitor : ' , visitor)


    #df = df[~df['AMAC'].isin(drop_list)]        #listのMACを削除
    df = df.query('AMAC not in @drop_list')      #query使ってMAC削除(こっちのが早いらしい)
    df = df.reset_index(drop=True)       #インデックス番号を振り直してる　なくてもいいかも
    df.at[0, 'Number of visitor'] = visitor


    '''
    ret = df.groupby('AMAC')['AMPID'].apply(lambda d: d.value_counts(normalize=True))
    for i in ret
'''

drop_random_closed()
drop_non_visitor()
df.to_csv("20191105_fixed.csv")
end_time = datetime.datetime.now()  #計測終了

#print(df.head)
#print(df.loc[0]['SC']) == df.loc[0, 'SC']

#print(df.iloc[len(df)-1])
print("process time is ",end_time-start_time)