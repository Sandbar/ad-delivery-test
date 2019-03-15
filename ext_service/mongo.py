from pymongo import MongoClient
import configparser as cfg
import urllib
import os
import common.utils as tool


config = cfg.ConfigParser()
config.read('config.ini')
# 建立MongoDB数据库连接
MONGO_USR = config.get('mongo_conf', 'mongo_usr')
MONGO_PWD = config.get('mongo_conf', 'mongo_pwd')
MONGO_HOST = config.get('mongo_conf', 'mongo_host')
MONGO_DBNAME = config.get('mongo_conf', 'mongo_dbname')
MONGO_CLUSTER = config.get('mongo_conf', 'mongo_cluster')
cluster_string = ''
if len(MONGO_CLUSTER) > 0:
    cluster_string = 'replicaSet=' + MONGO_CLUSTER + '&'
mongo_server_uri = 'mongodb://' + urllib.parse.quote_plus(MONGO_USR) + ':' + \
                   urllib.parse.quote_plus(MONGO_PWD) + '@' + MONGO_HOST + \
                   '/' + '?' + cluster_string + 'authSource=' + MONGO_DBNAME
os.environ["mongo_host"] = MONGO_HOST
os.environ["mongo_dbname"] = MONGO_DBNAME
os.environ["mongo_usr"] = MONGO_USR
os.environ["mongo_pwd"] = MONGO_PWD
os.environ["mongo_cluster"] = MONGO_CLUSTER


def record_blood(ads_group):
    client = MongoClient(mongo_server_uri)
    # 连接所需数据库,test为数据库名
    db = client.get_database(os.environ['mongo_dbname'])
    for ad_id in ads_group:
        db.blood.insert_one({
            'ad_id': ad_id,
            'blood': ads_group[ad_id],
            'update': tool.get_time_now()
        })
    client.close()


def blood_return(ad_id, blood, ret_type):
    client = MongoClient(mongo_server_uri)
    # 连接所需数据库,test为数据库名
    db = client.get_database(os.environ['mongo_dbname'])
    db.blood_return.insert_one({
        'type': ret_type,
        'ad_id': ad_id,
        'blood': blood,
        'apply_date': tool.get_today(),
        'update': tool.get_time_now()
    })
    client.close()


def check_period(ad_id):
    client = MongoClient(mongo_server_uri)
    db = client.get_database(os.environ['mongo_dbname'])
    out = db.blood_return.find({'ad_id': ad_id, 'apply_date': tool.get_today(), 'type': 'period'})
    res_len = len(list(out))
    client.close()
    if res_len > 0:
        return True
    else:
        return False


def check_prize(ad_id):
    client = MongoClient(mongo_server_uri)
    db = client.get_database(os.environ['mongo_dbname'])
    out = db.blood_return.find({'ad_id': ad_id, 'apply_date': tool.get_today(), 'type': 'prize'})
    res_len = len(list(out))
    client.close()
    if res_len > 0:
        return True
    else:
        return False


def update_delivery_data(today, spend, install, pay, remove_no_spend_ads,
                         pause_has_spend_ads, cur_alive_ads, cur_dead_ads,
                         apply_ads_count, awake_ads_count, remove_pending4h_ads, gt6):
    client = MongoClient(mongo_server_uri)
    db = client.get_database(os.environ['mongo_dbname'])
    db.delivery_data.update_one(
        {'delivery_name': os.environ["name"], 'report_date': today},
        {
            '$set': {
                'report_date': today,
                'delivery_name': os.environ["name"],
                'cur_alive_ads': cur_alive_ads,
                'cur_dead_ads': cur_dead_ads,
                'relevance_gte6_count': gt6
            },
            '$inc': {
                'spend': spend,
                'install': install,
                'pay': pay,
                'remove_no_spend_ads': remove_no_spend_ads,
                'pause_has_spend_ads': pause_has_spend_ads,
                'apply_ads_count': apply_ads_count,
                'awake_ads_count': awake_ads_count,
                'remove_pending4h_ads': remove_pending4h_ads
            }
        },
        True
    )
    client.close()


def record_insights(ad_dict):
    client = MongoClient(mongo_server_uri)
    # 连接所需数据库,test为数据库名
    db = client.get_database(os.environ['mongo_dbname'])
    for ad_id in ad_dict:
        spend, install, pay = ad_dict[ad_id]
        if spend > 0:
            db.insights.insert_one({
                'delt_name': os.environ["name"],
                'ad_id': ad_id,
                'spend': spend,
                'install': install,
                'pay': pay,
                'update': tool.get_time_now()
            })
    client.close()


def get_campaigns_create_time(campaign_array):
    client = MongoClient(mongo_server_uri)
    # 连接所需数据库,test为数据库名
    db = client.get_database(os.environ['mongo_dbname'])
    out = db.ads.find({'campaign_id': {'$in': campaign_array}},
                      {"create_time": 1, "ad_id": 1, "campaign_id": 1, "_id": 0})
    tmp = list(out)
    client.close()
    return tmp


def get_delt_info(delt_name):
    client = MongoClient(mongo_server_uri)
    # 连接所需数据库,test为数据库名
    db = client.get_database(os.environ['mongo_dbname'])
    out = db.delivery.find({'name': delt_name})
    tmp = list(out)
    client.close()
    return tmp


def get_resource_info():
    client = MongoClient(mongo_server_uri)
    # 连接所需数据库,test为数据库名
    db = client.get_database(os.environ['mongo_dbname'])
    out = db.resource.find({})
    tmp = list(out)
    client.close()
    return tmp


def get_access_token(account_id):
    client = MongoClient(mongo_server_uri)
    # 连接所需数据库,test为数据库名
    db = client.get_database(os.environ['mongo_dbname'])
    out = db.account.find({"account_id": account_id})
    tmp = list(out)[0]['access_token']
    client.close()
    return tmp


def unique_check(pt_key):
    client = MongoClient(mongo_server_uri)
    # 连接所需数据库,test为数据库名
    db = client.get_database(os.environ['mongo_dbname'])
    out = db.ads.find({"pt_hash": pt_key})
    tmp = list(out)
    client.close()
    if len(tmp) > 0:
        return False
    else:
        return True


def stats_trigger():
    client = MongoClient(mongo_server_uri)
    # 连接所需数据库,test为数据库名
    db = client.get_database(os.environ['mongo_dbname'])
    out = db.income_logs.find({"report_date": tool.get_yesterday()})
    tmp = list(out)
    client.close()
    if len(tmp) > 0:
        return True
    else:
        return False


def get_yesterday_effect(delt_name):
    client = MongoClient(mongo_server_uri)
    # 连接所需数据库, ai_explore为数据库名
    db = client.get_database(os.environ['mongo_dbname'])
    out = db.insights.find({'delt_name': delt_name,
                            'update': {'$gte': tool.get_yesterday() + ' 00:00:00',
                                       '$lte': tool.get_yesterday() + ' 23:59:59'}
                            }).distinct("ad_id")
    fetch_out = db.incomes.find({"ad_id": {"$in": list(out)},
                                 'report_date': tool.get_yesterday(),
                                 'daily_install_count': {'$gte': 5}, 'roi': {'$gte': 0.02}},
                                {'ad_id': 1, 'daily_install_count': 1, 'roi': 1, '_id': 0})
    tmp = list(fetch_out)
    client.close()
    return tmp


def remove_ads(ads_array):
    client = MongoClient(mongo_server_uri)
    db = client.get_database(os.environ['mongo_dbname'])
    db.ads.remove({'ad_id': {'$in': ads_array}})
    client.close()


def get_adset_id(ad_id):
    client = MongoClient(mongo_server_uri)
    db = client.get_database(os.environ['mongo_dbname'])
    out = db.ads.find({'ad_id': ad_id})
    tmp = list(out)[0]['adset_id']
    client.close()
    return tmp


def get_campaign_id(ad_id):
    client = MongoClient(mongo_server_uri)
    db = client.get_database(os.environ['mongo_dbname'])
    out = db.ads.find({'ad_id': ad_id})
    tmp = list(out)[0]['campaign_id']
    client.close()
    return tmp


def get_ads_id(campaigns):
    client = MongoClient(mongo_server_uri)
    db = client.get_database(os.environ['mongo_dbname'])
    out = db.ads.find({'campaign_id': {'$in': campaigns}}, {'ad_id': 1, '_id': 0})
    tmp = list(out)
    ads = []
    for row in tmp:
        ads.append(row['ad_id'])
    client.close()
    return ads


def get_campaigns_id(ads_id):
    client = MongoClient(mongo_server_uri)
    db = client.get_database(os.environ['mongo_dbname'])
    out = db.ads.find({'ad_id': {'$in': ads_id}}, {'campaign_id': 1, '_id': 0})
    tmp = list(out)
    campaigns = []
    for row in tmp:
        campaigns.append(row['campaign_id'])
    client.close()
    return campaigns


def get_ads_dict(campaigns):
    client = MongoClient(mongo_server_uri)
    db = client.get_database(os.environ['mongo_dbname'])
    out = db.ads.find({'campaign_id': {'$in': campaigns}},
                      {'ad_id': 1, "campaign_id": 1, '_id': 0})
    tmp = list(out)
    ads = dict()
    for row in tmp:
        ads[row['ad_id']] = row['campaign_id']
    client.close()
    return ads


def get_blood_func_args_by_delivery(delt_name):
    client = MongoClient(mongo_server_uri)
    db = client.get_database(os.environ['mongo_dbname'])
    out = db.delivery.find({'name': delt_name}, {"_id": 0, "blood_func": 1})
    record = list(out)[0]
    client.close()
    return record['blood_func']


def get_deliverys():
    client = MongoClient(mongo_server_uri)
    db = client.get_database(os.environ['mongo_dbname'])
    out = db.delivery.find({}, {"_id": 0, "name": 1})
    records = list(out)
    client.close()
    delt_names = list()
    for row in records:
        delt_names.append(row['name'])
    return delt_names


def get_bid_trace_tuple(ad_id):
    client = MongoClient(mongo_server_uri)
    db = client.get_database(os.environ['mongo_dbname'])
    out = db.bid_trace.find({'ad_id': ad_id, 'date': tool.get_today()})
    records = list(out)
    client.close()
    max_spend = 0
    install = 0
    pay = 0
    for row in records:
        if row['spend'] > max_spend:
            max_spend = row['spend']
            install = row['install']
            pay = row['pay']
    return max_spend, install, pay


def record_bid_trace(ad_id, spend, install, pay, cur_bid):
    client = MongoClient(mongo_server_uri)
    # 连接所需数据库,test为数据库名
    db = client.get_database(os.environ['mongo_dbname'])
    db.bid_trace.insert_one({
        'delt_name': os.environ["name"],
        'ad_id': ad_id,
        'spend': spend,
        'install': install,
        'pay': pay,
        'bid_amount': cur_bid,
        'date': tool.get_today(),
        'update': tool.get_time_now()
    })
    client.close()
