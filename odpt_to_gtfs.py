import pandas as pd
import json
import requests
from tqdm import tqdm
import get_env

base_url = 'https://api-tokyochallenge.odpt.org/api/v4/'
API_KEY = get_env.API_KEY

def make_station_id_from_odpt_station_id(odpt_station_id):
    return odpt_station_id.split(':')[1].split('.')[0] + '.' +odpt_station_id.split(':')[1].split('.')[2]

# Stationのデータフレームと駅リストを作る
station_list = []
url = base_url + 'odpt:Station.json'
query = {
        'acl:consumerKey': API_KEY,
        }
response = requests.get(url, params=query)
station_df = pd.read_json(response.text) # あとで使うのでついでに

for station_obj in response.json():
    if "geo:lat" not in station_obj.keys():
        continue
    station_id = make_station_id_from_odpt_station_id(station_obj["owl:sameAs"])
    station_dict = {
    'stop_id': station_id,
    'stop_name': station_obj["odpt:stationTitle"]["ja"],
    'stop_lat': station_obj["geo:lat"],
    'stop_lon': station_obj["geo:long"],
    'zone_id': station_id
    }
    station_list.append(station_dict)

# 駅情報のデータフレームを作る
stops_df = pd.io.json.json_normalize(station_list).drop_duplicates(subset='stop_id').reset_index(drop=True)
# stops.txtを出力する
stops_df.to_csv("./output/stops.txt", index=False)
print("stops.txtを出力しました")


url = base_url + '/odpt:TrainTimetable.json'
query = {
        'acl:consumerKey': API_KEY,
        }
redirect_url = requests.get(url, params=query)
response = requests.get(redirect_url.url, params=query)
train_timetables = response.json()

#JR東の特急を除く処理
train_timetables = [item for item in train_timetables if item["odpt:trainType"] != 'odpt.TrainType:JR-East.LimitedExpress']

#previous_and_nextから特定の路線を除く処理
def remove_specific_timetable_in_previous_and_next(item, line_name):
    if 'odpt:previousTrainTimetable' in item.keys():
        item['odpt:previousTrainTimetable'] = [item for item in item['odpt:previousTrainTimetable'] if item.split(':')[1].split('.')[1] != line_name]
        if item['odpt:previousTrainTimetable'] == []:
            item.pop('odpt:previousTrainTimetable')

    if 'odpt:nextTrainTimetable' in item.keys():
        item['odpt:nextTrainTimetable'] = [item for item in item['odpt:nextTrainTimetable'] if item.split(':')[1].split('.')[1] != line_name]
        if item['odpt:nextTrainTimetable'] == []:
            item.pop('odpt:nextTrainTimetable')
    return item

#previous_and_nextから湘南新宿ラインを除く処理
train_timetables = [remove_specific_timetable_in_previous_and_next(item, 'ShonanShinjuku') for item in train_timetables]


print('分割がある列車に関する処理')
# 分割がある列車について、前列車最終駅と後列車最初駅が一致する後列車を前列車のnextに、もう一方の後ろ列車はpreviousを消して普通の列車にする
ignore_previous_list = []
def optimize_nextTrainTimetable_of_train_will_be_divided(train_will_be_divided):
    next_train_ids = train_will_be_divided['odpt:nextTrainTimetable']
    ignore_previous_list.append(next_train_ids[1])
    train_will_be_divided['odpt:nextTrainTimetable'].pop(1)
    return train_will_be_divided
train_timetables = [optimize_nextTrainTimetable_of_train_will_be_divided(item) if 'odpt:nextTrainTimetable' in item.keys() and len(item['odpt:nextTrainTimetable']) == 2 else item for item in train_timetables]
train_timetables = [item if item not in ignore_previous_list else item.pop('odpt:previousTrainTimetable') for item in train_timetables]

print('併合がある列車に関する処理')
# 併合される列車について、前列車最終駅と後列車最初駅が一致する前列車を後列車のpreviousに、もう一方の前列車はnextを消して普通の列車にする
ignore_next_list = []
def optimize_previousTrainTimetable_of_train_connected(train_connected):
    previous_train_ids = train_connected['odpt:previousTrainTimetable']
    ignore_next_list.append(previous_train_ids[1])
    train_connected['odpt:previousTrainTimetable'].pop(1)
    return train_connected
train_timetables = [optimize_previousTrainTimetable_of_train_connected(item) if 'odpt:previousTrainTimetable' in item.keys() and len(item['odpt:previousTrainTimetable']) == 2 else item for item in train_timetables]
train_timetables = [item if item not in ignore_next_list else item.pop('odpt:nextTrainTimetable') for item in train_timetables]

# trainTimetableObjectの中にソースとなるtrain_idを付与、元の駅名から路線名を抜いたstation_idを付与、
# 通過する路線を保持するpass_railway_listを追加、マージしたtrain_timetableのtrain_idを保持するmerged_train_id_listを追加(マージされない時は1個入ってる)
def optimize_station_info_in_train_timetable(train_timetable_obj):
    for item in train_timetable_obj["odpt:trainTimetableObject"]:
        if 'odpt:arrivalStation' in item.keys():
            item['arrival_station_data_resource'] = train_timetable_obj["owl:sameAs"]
            item['arrival_station_id'] = make_station_id_from_odpt_station_id(item['odpt:arrivalStation'])
        if 'odpt:departureStation' in item.keys():
            item['departure_station_data_resource'] = train_timetable_obj["owl:sameAs"]
            item['departure_station_id'] = make_station_id_from_odpt_station_id(item['odpt:departureStation'])
    train_timetable_obj['pass_railway_list'] = [train_timetable_obj['odpt:railway']]
    train_timetable_obj['merged_train_id_list'] = [train_timetable_obj['owl:sameAs']]

    #odpt:previousTrainTimetable を配列から出す(前段階の処理で1個になっている)
    #前が他社だった時、previous_direct_train_id_of_other_operatorにtrain_idを入れる
    train_timetable_obj['previous_direct_train_id_of_other_operator'] = ''
    if 'odpt:previousTrainTimetable' in train_timetable_obj.keys():
        train_timetable_obj['odpt:previousTrainTimetable'] = train_timetable_obj['odpt:previousTrainTimetable'][0]
        if train_timetable_obj['odpt:previousTrainTimetable'].split(':')[1].split('.')[0] !=  train_timetable_obj['odpt:operator'].split(':')[1]:
            train_timetable_obj['previous_direct_train_id_of_other_operator'] = train_timetable_obj['odpt:previousTrainTimetable']

    #odpt:nextTrainTimetable を配列から出す(前段階の処理で1個になっている)
    #後が他社だった時、next_direct_train_id_of_other_operatorにtarin_idを入れる
    train_timetable_obj['next_direct_train_id_of_other_operator'] = ''
    if 'odpt:nextTrainTimetable' in train_timetable_obj.keys():
        train_timetable_obj['odpt:nextTrainTimetable'] = train_timetable_obj['odpt:nextTrainTimetable'][0]
        if train_timetable_obj['odpt:nextTrainTimetable'].split(':')[1].split('.')[0] !=  train_timetable_obj['odpt:operator'].split(':')[1]:
            train_timetable_obj['next_direct_train_id_of_other_operator'] = train_timetable_obj['odpt:nextTrainTimetable']

    return train_timetable_obj

train_timetables = [optimize_station_info_in_train_timetable(item) for item in train_timetables]


#路線をまたぐ列車のダイヤを１つにマージする。
print("路線をまたぐ列車のダイヤを１つにマージする作業")

merged_train_timetables = []
for forward_timetable in tqdm(train_timetables): # tqdm はプログレスバー用

    #前:自社 → スルー
    if 'odpt:previousTrainTimetable' in forward_timetable.keys() \
    and forward_timetable['previous_direct_train_id_of_other_operator'] == '':
        continue

    #前:無し 後:無し  または
    #前:他社 後:無し  →そのままリストへ
    if 'odpt:nextTrainTimetable' not in forward_timetable.keys():
        merged_train_timetables.append(forward_timetable)
        continue

    #前:無し 後:他社  または
    #前:他社 後:他社  →そのままリストへ
    if forward_timetable['next_direct_train_id_of_other_operator'] != '':
        merged_train_timetables.append(forward_timetable)
        continue

    #前:無し 後:自社  または
    #前:他社 後:自社  → 連結してリストへ
    if 'odpt:nextTrainTimetable' in forward_timetable.keys() \
    and forward_timetable['next_direct_train_id_of_other_operator'] == '':
        temp_forward_timetable = forward_timetable
        while True:
            backward_timetable = [item for item in train_timetables if item['owl:sameAs'] == temp_forward_timetable['odpt:nextTrainTimetable']][0]

            #odpt:trainTimetableObjectをマージ
            #前列車の最終到着駅と後列車の最初出発駅の情報が揃っている
            if 'odpt:arrivalStation' in temp_forward_timetable['odpt:trainTimetableObject'][-1].keys() \
            and 'odpt:departureStation' in backward_timetable['odpt:trainTimetableObject'][0].keys() \
            and make_station_id_from_odpt_station_id(temp_forward_timetable['odpt:trainTimetableObject'][-1]['odpt:arrivalStation']) == make_station_id_from_odpt_station_id(backward_timetable['odpt:trainTimetableObject'][0]['odpt:departureStation']):
                temp_forward_timetable['odpt:trainTimetableObject'][-1].update(backward_timetable['odpt:trainTimetableObject'][0])
                temp_forward_timetable['odpt:trainTimetableObject'] += backward_timetable['odpt:trainTimetableObject'][1:]
            else:
                temp_forward_timetable['odpt:trainTimetableObject'] += backward_timetable['odpt:trainTimetableObject']

            #backward_timetableの後が無し
            if 'odpt:nextTrainTimetable' not in backward_timetable.keys():
                temp_forward_timetable.pop('odpt:nextTrainTimetable')
                temp_forward_timetable['pass_railway_list'].append(backward_timetable['odpt:railway'])
                temp_forward_timetable['merged_train_id_list'].append(backward_timetable['owl:sameAs'])
                merged_train_timetables.append(temp_forward_timetable)
                break

            #backward_timetableの後が他社
            if backward_timetable['next_direct_train_id_of_other_operator'] != '':
                temp_forward_timetable['odpt:nextTrainTimetable'] = backward_timetable['odpt:nextTrainTimetable']
                temp_forward_timetable['pass_railway_list'].append(backward_timetable['odpt:railway'])
                temp_forward_timetable['merged_train_id_list'].append(backward_timetable['owl:sameAs'])
                merged_train_timetables.append(temp_forward_timetable)
                break

            #backward_timetableの後が自社
            if 'odpt:nextTrainTimetable' in backward_timetable.keys()\
            and backward_timetable['next_direct_train_id_of_other_operator'] == '':
                temp_forward_timetable['pass_railway_list'].append(backward_timetable['odpt:railway'])
                temp_forward_timetable['merged_train_id_list'].append(backward_timetable['owl:sameAs'])
                temp_forward_timetable['odpt:nextTrainTimetable'] = backward_timetable['odpt:nextTrainTimetable']
                continue
        continue
print("路線をまたぐ列車のダイヤを１つにマージする作業が完了")

# trip.txtを作るための中間テーブルstop_station_dfを作る
stop_station_df_header = stops_df['stop_id'].tolist() + ['type_of_train','rail_direction', 'trip_id']
stop_station_dicts = []
#複数のtripを1つのrouteにするためのリストを作る
for train_dict in merged_train_timetables: #
    dict_of_stops = dict(zip(stop_station_df_header, [False] * len(stops_df['stop_id'].tolist()) + [train_dict["odpt:trainType"], train_dict["odpt:railDirection"], train_dict["owl:sameAs"]]))
    for train_timetable_obj in train_dict["odpt:trainTimetableObject"]:
        if "departure_station_id" in train_timetable_obj.keys():
            station_id = train_timetable_obj["departure_station_id"]
        else:
            station_id = train_timetable_obj["arrival_station_id"]
        dict_of_stops[station_id] = True
    stop_station_dicts.append(dict_of_stops)
stop_station_df = pd.DataFrame.from_dict(stop_station_dicts, orient='columns')
print("中間テーブルを作成")

# stop_station_dfから停車パターンが同じ列車のtrain_idを集めたリストtrains_stop_same_stations_listを作る
group_by_keys = stop_station_df.columns.values.tolist()
group_by_keys.remove('trip_id')
stop_pattern_series = stop_station_df.groupby(group_by_keys).agg({'trip_id': list})
trains_stop_same_stations_list =  [x[0] for x in stop_pattern_series.values.tolist()] #stop_pattern_series.values.tolist() で出てくるlistは各要素が二重に配列に入っているためそれを取り出して配列にし直している

# trains_stop_same_stations_listからroutes.txt, trips.txt, stop_times.txtを作る
routes_dicts = []
trips_dicts = []
stop_times_dicts = []

print("trains_stop_same_stations_listからroutes.txt, trips.txt, stop_times.txtを作る")
# Railwayのデータフレームを作る
url = base_url + 'odpt:Railway'
query = {
        'acl:consumerKey': API_KEY
        }
response = requests.get(url, params=query)
railway_df = pd.read_json(response.text)

# TrainTypeのデータフレームを作る
url = base_url + 'odpt:TrainType'
query = {
        'acl:consumerKey': API_KEY
        }
response = requests.get(url, params=query)
train_type_df = pd.read_json(response.text)

def make_time_str_in_gtfs_rule(origin_time_str):
    if int(origin_time_str.split(':')[0]) >= 3:
        hhmm = origin_time_str
    else:
        hhmm = str(int(origin_time_str.split(':')[0]) + 24) + ':' + origin_time_str.split(':')[1]
    return hhmm + ':00'

for trains_stop_same_station in tqdm(trains_stop_same_stations_list): # tqdm はプログレスバー用
    for index, trip_id in enumerate(trains_stop_same_station):
        train_timetable = [item for item in merged_train_timetables if item['owl:sameAs'] == trip_id][0]
        if index == 0: #リストの先頭の時はroute.txt用のデータを作る
            route_id = len(routes_dicts)
            # {路線名}{種別} {終着駅}行き
            rail_name                = railway_df[railway_df['owl:sameAs'] == train_timetable['odpt:railway']].iloc[0]['dc:title']
            train_type               = train_type_df[train_type_df['owl:sameAs'] == train_timetable['odpt:trainType']].iloc[0]['dc:title']
            if 'odpt:destinationStation' in train_timetable.keys():
                destination_station_name = station_df[station_df['owl:sameAs'] == train_timetable['odpt:destinationStation'][0]].iloc[0]['dc:title']
            else:
                destination_station_name = station_df[station_df['owl:sameAs'] == train_timetable['odpt:trainTimetableObject'][-1]['odpt:arrivalStation']].iloc[0]['dc:title']
            route_long_name =  f'{rail_name}{train_type} {destination_station_name}行き'
            route_dict = {
            'route_id': route_id,
            'agency_id': train_timetable['odpt:operator'],
            'route_short_name': '',
            'route_long_name': route_long_name,
            'route_type': 2
            }
            routes_dicts.append(route_dict)

        trip_dict = {
        'route_id': route_id,
        'service_id': train_timetable['odpt:calendar'],
        'trip_id': trip_id,
        'trip_headsign': '',
        }
        trips_dicts.append(trip_dict)

        for index, timetable_object in enumerate(train_timetable['odpt:trainTimetableObject']):
            if 'odpt:arrivalTime' in timetable_object.keys():
                arrival_time = make_time_str_in_gtfs_rule(timetable_object["odpt:arrivalTime"])
            if 'odpt:departureTime' in timetable_object.keys():
                departureTime = make_time_str_in_gtfs_rule(timetable_object["odpt:departureTime"])
            else:
                departureTime = arrival_time
            if 'odpt:arrivalTime' not in timetable_object.keys():
                arrival_time = departureTime

            if index == 0: #始発駅の設定
                pickup_type = 0
                drop_off_type = 1
            elif index == len(train_timetable['odpt:trainTimetableObject']) - 1:  #終着駅の設定
                pickup_type = 1
                drop_off_type = 0
            else:
                pickup_type = 0
                drop_off_type = 0

            if "odpt:departureStation" in timetable_object.keys():
                station = timetable_object["odpt:departureStation"]
            else:
                station = timetable_object["odpt:arrivalStation"]
            stop_id = make_station_id_from_odpt_station_id(station)
            stop_time_dict = {
            'trip_id': trip_id,
            'arrival_time': arrival_time,
            'departure_time': departureTime,
            'stop_id': stop_id,
            'stop_sequence': index,
            'stop_headsign': '',
            'pickup_type': pickup_type,
            'drop_off_type': drop_off_type
            }
            stop_times_dicts.append(stop_time_dict)

routes_df = pd.DataFrame.from_dict(routes_dicts, orient='columns')
routes_df.to_csv("./output/routes.txt", index=False)
print("routes.txtを出力しました")

trips_df = pd.DataFrame.from_dict(trips_dicts, orient='columns')
trips_df.to_csv("./output/trips.txt", index=False)
print("trips.txtを出力しました")

stop_times_df = pd.DataFrame.from_dict(stop_times_dicts, orient='columns')
stop_times_df.to_csv("./output/stop_times.txt", index=False)
print("stop_times.txtを出力しました")
