from common import utils as tool
import os
import json
import urllib3
import requests
import time
import ext_service.nats_proc as ntp
import common.logger as log


# 根据query返回不同要求的广告集合
def get_campaigns(query):
    urllib3.disable_warnings()
    campaigns = list()
    dst_url = tool.FB_HOST_URL + os.environ["act_id"] + '/campaigns?{}'.format(query)
    while True:
        try:
            html = requests.get(dst_url, verify=False).text
            json_out = json.loads(html)
            if 'data' not in json_out:
                time.sleep(5)
            else:
                for node in json_out['data']:
                    campaign_id = node['id']
                    adset_id = node['adsets']['data'][0]['id']
                    ad_id = node['adsets']['data'][0]['ads']['data'][0]['id']
                    campaigns.append((campaign_id, adset_id, ad_id))
                if 'paging' in json_out:
                    if 'next' in json_out['paging']:
                        dst_url = json_out['paging']['next']
                    else:
                        break
                else:
                    break
        except Exception as e:
            log.logger.info(str(e))
            log.log_exp()
            time.sleep(60)
    return campaigns


# 获取当前账号下所有状态为PAUSED的广告
def get_paused_campaigns():
    out = get_campaigns_custom(None, {'key': 'effective_status', 'value': ['PAUSED']})
    campaigns = list()
    for value in out:
        campaigns.append(value[0])
    return campaigns


def get_ads_paused_campaigns():
    out = get_campaigns_custom(None, {'key': 'ad.effective_status', 'value': ['PAUSED']})
    campaigns = list()
    for value in out:
        campaigns.append(value[0])
    return campaigns


# 获取当前投放中状态为ACTIVE的广告
def get_active_ads_by_delivery():
    out = get_campaigns_custom(os.environ["name"].upper(), {'key': 'ad.effective_status', 'value': ['ACTIVE']})
    ads = dict()
    for value in out:
        ads[(value[2])] = (value[0], value[1])
    return ads


def get_active_campaigns():
    out = get_campaigns_custom(None, {'key': 'ad.effective_status', 'value': ['ACTIVE']})
    campaigns = list()
    for value in out:
        campaigns.append(value[0])
    return campaigns


def get_paused_ads_by_delivery():
    out = get_campaigns_custom(os.environ["name"].upper(), {'key': 'effective_status', 'value': ['PAUSED']})
    ads = dict()
    for value in out:
        ads[value[2]] = (value[0], value[1])
    return ads


def get_pending_view_campaigns_by_delivery():
    out = get_campaigns_custom(os.environ["name"].upper(), {'key': 'ad.effective_status', 'value': ['PENDING_REVIEW']})
    campaigns = list()
    for value in out:
        campaigns.append(value[0])
    return campaigns


def get_pending_view_campaigns():
    out = get_campaigns_custom(None, {'key': 'ad.effective_status', 'value': ['PENDING_REVIEW']})
    campaigns = list()
    for value in out:
        campaigns.append(value[0])
    return campaigns


def get_campaigns_custom(sub_name, status):
    filter_array = list()
    if sub_name is not None:
        filter_array.append({'field': 'name', 'operator': 'CONTAIN', 'value': sub_name})
    filter_array.append({'field': status['key'], 'operator': 'IN', 'value': status['value']})
    query = tool.compose({'fields': 'name,adsets{ads}', 'limit': tool.LIMIT_MAX, "filtering": filter_array})
    return get_campaigns(query)


def get_disapproved_campaigns_by_delivery():
    out = get_campaigns_custom(os.environ["name"].upper(), {'key': 'ad.effective_status', 'value': ['DISAPPROVED']})
    campaigns = list()
    for value in out:
        campaigns.append(value[0])
    return campaigns


def stop_campaign_process(stop_campaigns, cmd_list):
    start = 0
    step = 100
    end = step
    n_len = len(stop_campaigns)
    if end <= n_len:
        while True:
            ntp.control_campaign(stop_campaigns[start:end], cmd_type=cmd_list[start:end])
            time.sleep(30)
            start = end
            end = start + step
            if end > n_len:
                end = n_len
            if start >= end:
                break
    else:
        ntp.control_campaign(stop_campaigns, cmd_type=cmd_list)

    if n_len > 0:
        log.logger.info("stop " + str(n_len) + " campaigns ...... finished!")


# 删除指定账号下状态为inactive的广告
def delete_in_active_campaigns():
    campaign_set = get_paused_campaigns()
    campaign_set_cmd = list()
    for i in range(0, len(campaign_set)):
        campaign_set_cmd.append("DELETED")
    log.logger.info("准备删除"+str(len(campaign_set))+"个状态为inactive的广告。")
    ntp.control_campaign(campaign_set, campaign_set_cmd)


def delete_active_campaigns():
    campaign_set = get_active_campaigns()
    campaign_set_cmd = list()
    for i in range(0, len(campaign_set)):
        campaign_set_cmd.append("DELETED")
    log.logger.info("准备删除" + str(len(campaign_set)) + "个状态为 active 的广告。")
    ntp.control_campaign(campaign_set, campaign_set_cmd)


def delete_pending_campaigns():
    campaign_set = get_pending_view_campaigns()
    campaign_set_cmd = list()
    for i in range(0, len(campaign_set)):
        campaign_set_cmd.append("DELETED")
    log.logger.info("准备删除" + str(len(campaign_set)) + "个状态为 pending view 的广告。")
    ntp.control_campaign(campaign_set, campaign_set_cmd)


# Not Delivering ads
def delete_ads_in_active_campaigns():
    campaign_set = get_ads_paused_campaigns()
    campaign_set_cmd = list()
    for i in range(0, len(campaign_set)):
        campaign_set_cmd.append("DELETED")
    log.logger.info("准备删除" + str(len(campaign_set)) + "个 ads 状态为inactive的广告。")
    ntp.control_campaign(campaign_set, campaign_set_cmd)


def delete_ads_custom(sub_name, status):
    for name in sub_name:
        campaign_set = get_campaigns_custom(name, status)
        campaign_set_cmd = list()
        for i in range(0, len(campaign_set)):
            campaign_set_cmd.append("DELETED")
        log.logger.info("准备删除" + str(len(campaign_set)) + "个 ads custom 的广告。")
        ntp.control_campaign(campaign_set, campaign_set_cmd)


def get_bid(adset_id_list):
    req_out = requests.post(os.environ["get_adset_bid_amount"], data=json.dumps({"adsetIds": adset_id_list}))
    adset_bid = json.loads(req_out.text)
    return adset_bid


def update_bid(new_bid, cur_bid, ad_set_id, ad_id):
    if new_bid > tool.BID_MAX:
        new_bid = tool.BID_MAX
    if new_bid < tool.BID_MIN:
        new_bid = tool.BID_MIN
    new_bid = int(round(new_bid, 0))
    update_bid_amount(ad_set_id, new_bid)
    log.logger.info("ad set " + ad_set_id + '<ad_id:' + ad_id +
                    '> adjust bid amount from ' + str(cur_bid) + " to " + str(new_bid))


def update_bid_amount(ad_set_id, bid_amount):
    url = tool.FB_HOST_URL + ad_set_id
    data = tool.compose({'bid_amount': bid_amount}, is_utf8=True)
    while True:
        try:
            req_out = requests.post(url, data=data)
            log.logger.info(req_out.text)
        except Exception as e:
            log.logger.info(e)
            log.log_exp()
            time.sleep(60)
        else:
            json_out = json.loads(req_out.text)
            if 'success' in json_out and json_out['success']:
                log.logger.info("update bid success!")
                log.logger.info('adset ' + ad_set_id + ' update bid amount to ' + str(bid_amount))
                break
            else:
                log.logger.info("update bid failure!")
                time.sleep(60)


def change_to_active(ads_dict):
    ready_campaigns = list()
    ready_cmd_types = list()
    for ad_id in ads_dict:
        ready_campaigns.append(ads_dict[ad_id])
        ready_cmd_types.append('ACTIVE')
    if len(ready_campaigns) > 0:
        ntp.control_campaign(ready_campaigns, ready_cmd_types)
