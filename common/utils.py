import json
from collections import OrderedDict
import os
import urllib
import time
from pytz import timezone, utc
import pytz
from datetime import datetime


def get_last_day_end_time():
    import datetime
    from datetime import timedelta
    yesterday = (datetime.datetime.now(tz) - timedelta(1)).strftime('%Y-%m-%d')
    return yesterday + ' 23:59:59'


def get_obj_key():
    import datetime
    parts = datetime.datetime.now(tz).strftime('%d-%m').split('-')
    day = int(parts[0])
    month = int(parts[1])
    key = str(day) + '-' + str(month)
    return key


def custom_time(*args):
    # 配置logger
    utc_dt = utc.localize(datetime.utcnow())
    my_tz = timezone("Asia/Shanghai")
    converted = utc_dt.astimezone(my_tz)
    return converted.timetuple()


def load_json(json_file):
    with open(json_file, 'r', encoding='UTF-8') as load_f:
        load_dict = json.load(load_f, object_pairs_hook=OrderedDict)
    return load_dict


def save_json(json_file, json_obj):
    with open(json_file, "w") as f:
        json.dump(json_obj, f, indent=4)


def compose(data, is_utf8=False):
    dic = {'access_token': os.environ["access_token"]}
    for key in data:
        dic[key] = data[key]
    query_str = urllib.parse.urlencode(dic)
    if is_utf8:
        return query_str.encode('utf-8')
    else:
        return query_str


def get_insights_by_json(json_out):
    if len(json_out['data']) == 0:
        return 0, 0, 0
    spend = float(json_out['data'][0]['spend'])
    install = 0
    pay = 0
    if 'actions' not in json_out['data'][0]:
        return spend, install, pay
    else:
        for act_type in json_out['data'][0]['actions']:
            if act_type['action_type'] == 'mobile_app_install':
                install = int(act_type['value'])
            if act_type['action_type'] == 'app_custom_event.fb_mobile_purchase':
                pay = int(act_type['value'])
        return spend, install, pay


def get_cur_hour():
    return int(time.strftime('%H', time.localtime(time.time())))


def get_time_now():
    import datetime
    time_now = datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    return time_now


def get_today():
    import datetime
    return datetime.datetime.now(tz).strftime('%Y-%m-%d')


def get_yesterday():
    return get_past_date(1)


def get_past_date(n):
    from datetime import timedelta
    import datetime
    past_date = (datetime.datetime.now(tz) - timedelta(n)).strftime('%Y-%m-%d')
    return past_date


def get_cur_date():
    import datetime
    return datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')


def get_string(list_obj):
    temp = ''
    for value in list_obj:
        temp = temp + value + ' '
    return temp


tz = pytz.timezone('Asia/Shanghai')
FB_API_VERSION = 'v3.0'
BID_MAX = 25000
BID_MIN = 1000
HEADERS = {'Content-Type': "application/json"}
THREAD_POOL_SIZE = 100
FB_HOST_URL = "https://graph.facebook.com/v3.0/"
LIMIT_MAX = 50
