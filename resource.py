import ext_service.mongo as mongo
import os


def get_config():
    info = mongo.get_resource_info()
    os.environ["nat_url"] = info[0]['nats_server']['nat_url']
    os.environ["overlap_nats_url"] = info[0]['nats_server']['overlap_nats_url']
    os.environ["at_users"] = info[0]['sms_conf']['at_users']
    os.environ["access_url"] = info[0]['sms_conf']['access_url']
    os.environ["reply_to"] = 'cador_delt_pro_stop_campaign'
    os.environ["period_blood_service"] = info[0]['service']['period_blood']
    os.environ["get_adset_bid_amount"] = info[0]['service']['get_adset_bid_amount']
    os.environ["gen_ads_service"] = info[0]['service']['gen_ads_service']
    os.environ["facebook_service"] = info[0]['service']['facebook_service']
