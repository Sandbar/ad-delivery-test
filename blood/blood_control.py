from common import utils as tool
import datetime
import ext_service.mongo as mongo
import ext_service.nats_proc as ntp
import os
import ext_service.sms_service as sms_service
import json
import math
import random
import requests
import ext_service.mother as mother
import data_center.snapshot as ds
import data_center.data_service as bsc
from copy import deepcopy
import common.logger as log
import ext_service.facebook as fb
import blood.blood_base as bb
import bid_rank.bid_control as bid

cur_index = {'active': 0, 'pending': 0, 'paused': 0, 'today': '', 'gt6': 0}


def awake(n):
    inactive_ads = ds.snap_data.inactive_ads_dict
    relevance_score = ds.snap_data.inactive_ads_relevance_score
    sub_ads = list()
    for ad_id in relevance_score:
        if relevance_score[ad_id] >= 6:
            sub_ads.append(ad_id)
    ads_xgs = fb.get_ads_insights(sub_ads, tool.get_past_date(5))
    select_ads = bb.filter_by_insights(ads_xgs, n)
    # 调整出价
    for key in select_ads:
        spend, install, pay = ads_xgs[key]
        ad_set_id = ds.snap_data.inactive_ads_dict[key][1]
        bid_amount = bsc.get_bid([ad_set_id])[0][ad_set_id]
        level = bid.get_bid_level(bid_amount)
        new_bid = 0
        if level == "L3":
            if pay >= 5 and spend/pay <= bid.cpp_min:
                log.logger.info("ad "+str(key)+" keep the bid amount.")
            else:
                new_bid = random.randint(bid.lvl2_min, bid.lvl2_max - 1)
        if level == "L2":
            if install >= 10 and spend/install <= bid.cpi_min:
                log.logger.info("ad "+str(key)+" keep the bid amount.")
            else:
                new_bid = random.randint(bid.lvl1_min, bid.lvl1_max-1)
        if level == "L1":
            new_bid = int(bid_amount - 2000*(1 - select_ads[key]))
            if new_bid < bid.lvl1_min:
                new_bid = bid.lvl1_min
        if new_bid > 0:
            log.logger.info(
                "awake adjust bid amount  -- ad : " + str(key) + " lvl : " + str(level) + " new bid:" + str(new_bid))
            bsc.update_bid_amount(ad_set_id, new_bid)
    # 激活广告
    for ad_id in select_ads:
        select_ads[ad_id] = inactive_ads[ad_id][0]
    bsc.change_to_active(select_ads)
    return len(select_ads)


def dead_check():
    dead_size = int(os.environ["dead_size"])
    if cur_index['paused'] > dead_size:
        # 删除多余的广告
        ads_list = ds.snap_data.inactive_ads_dict.keys()
        ads_index = fb.get_ads_insights(ads_list, tool.get_past_date(6))
        select_ads = bb.filter_by_insights(ads_index, dead_size)
        remove_ads = list(set(ads_list) ^ set(select_ads.keys()))
        remove_campaigns = mongo.get_campaigns_id(remove_ads)
        log.logger.info("stop " + str(len(remove_campaigns)) + " campaigns for over dead size.")
        bsc.stop_campaign_process(remove_campaigns, ["DELETED"]*len(remove_campaigns))


def online_check():
    # 获取状态为ACTIVE的广告数量
    online_count = cur_index['active'] + cur_index['pending']
    set_size = int(os.environ["init_ads_size"])
    if bb.delivery_status == "off":
        set_size = 0
    else:
        set_size = int(round(set_size*math.pow((5-bb.blood_control_coef)/4, 3), 0))
    log.logger.info("online ads:" + str(online_count))
    if online_count < set_size:
        ready_to_create = set_size - online_count
        log.logger.info("ready to create " + str(ready_to_create) + " ads.")
        try:
            if bb.blood_control_coef >= 2:
                log.logger.info('directly apply...')
                real_n = mother.create_new_ads(ready_to_create)
                mongo.update_delivery_data(cur_index['today'], 0, 0, 0, 0, 0, online_count,
                                           cur_index['paused'], real_n, 0, 0, cur_index['gt6'])
            else:
                need_awake_num = round(ready_to_create)
                real_awake_num = awake(need_awake_num)
                log.logger.info('awake '+str(real_awake_num)+' ads.')
                real_n = mother.create_new_ads(ready_to_create - real_awake_num)
                mongo.update_delivery_data(cur_index['today'], 0, 0, 0, 0, 0, online_count,
                                           cur_index['paused'], real_n, real_awake_num, 0, cur_index['gt6'])
        except Exception as e:
            log.logger.info("create ads exception:" + str(e))
            log.log_exp()


def status_check():
    # 去掉状态为 In Review 且发布时间超过4小时的广告
    pending_campaigns = ds.snap_data.pending_campaigns
    disapproved_campaigns = ds.snap_data.disapproved_campaigns
    need_remove_campaigns = []
    need_remove_campaigns_cmd_type = []
    need_remove_ads = []
    if len(pending_campaigns) > 0:
        time_now = datetime.datetime.now(tool.tz).strftime('%Y-%m-%d %H:%M:%S')
        new_date = datetime.datetime.strptime(time_now, '%Y-%m-%d %H:%M:%S')
        ads_time = mongo.get_campaigns_create_time(pending_campaigns)
        for record in ads_time:
            old_date = datetime.datetime.strptime(record['create_time'], '%Y-%m-%d %H:%M:%S')
            diff_val = new_date - old_date
            if diff_val.seconds / 3600 + diff_val.days * 24 >= 4:
                need_remove_campaigns.append(record['campaign_id'])
                need_remove_campaigns_cmd_type.append('DELETED')
                need_remove_ads.append(record['ad_id'])
    if len(need_remove_campaigns) > 0:
        ntp.control_campaign(need_remove_campaigns, cmd_type=need_remove_campaigns_cmd_type)
        mongo.remove_ads(need_remove_ads)
        log.logger.info(" remove "+str(len(need_remove_campaigns)) +
                        " campaigns for staying In Review status over 4 hours.")
    pending_campaigns_count = len(pending_campaigns) - len(need_remove_campaigns)
    # 发现状态为 Not Approved 的广告触发报警机制
    if len(disapproved_campaigns) > 0:
        message = "discover " + str(len(disapproved_campaigns)) + " ads Not Approved. from : " + os.environ["name"]
        try:
            sms_service.gsm_message(message)
            log.logger.info(message)
        except Exception as e:
            log.logger.info(str(e))
            log.log_exp()
    return pending_campaigns_count, len(need_remove_campaigns)


def period_control():
    data = {
        "delt_name": os.environ["name"],
        "utc8_datetime": tool.get_cur_date(),
        "period": "12"
    }
    try:
        req_out = requests.post(os.environ["period_blood_service"], data=json.dumps(data))
        json_out = json.loads(req_out.text)
        for element in json_out:
            if not mongo.check_prize(element['ad_id']) and not mongo.check_period(element['ad_id']):
                if element['ad_id'] in bb.ads_group_blood:
                    # 针对活跃的广告，周期回血
                    bb.set_blood(element['ad_id'], element['backBlood'], 0)
                    mongo.blood_return(element['ad_id'], element['backBlood'], 'period')
                    log.logger.info("周期回血 -> ad_id(" + element['ad_id'] + ") add blood:" + str(element['backBlood']))
    except Exception as e:
        log.logger.info("period blood exception:"+str(e))
        log.log_exp()


def prize_control():
    if mongo.stats_trigger() and bb.stats_trigger_date != tool.get_today():
        bb.stats_trigger_date = tool.get_today()
        effect_data = mongo.get_yesterday_effect(os.environ["name"])
        if len(effect_data) > 0:
            log.logger.info("触发奖励....")
            log.logger.info(effect_data)
            for effect in effect_data:
                install = effect['daily_install_count']
                roi = effect['roi']
                effect_ad_id = effect['ad_id']
                # 判断当前广告状态，只有active或paused的广告才能更改出价，调整血量
                out = fb.get_ads_status([effect_ad_id])
                if effect_ad_id in out:
                    cur_status = out[effect_ad_id]
                    if cur_status == 'ACTIVE':
                        if not mongo.check_prize(effect_ad_id):
                            coef = math.sqrt(bb.install_stats.cdf(install) * bb.roi_stats.cdf(roi))
                            # 奖励回血
                            back_blood = round(100 * coef)
                            bb.set_blood(effect_ad_id, back_blood, 0)
                            log.logger.info("奖励回血 -> ad_id(" + effect_ad_id + ") add blood:" + str(back_blood))
                            # 分析是否调整为VALUE模式
                            # 0.如果满足条件，则进入1，否则进入出价控制
                            # 1.判断广告名称中是否包含[VALUE yyyy-mm-dd]，如果满足条件，则进入2，否则不用调整
                            # 2.调用优化调整服务，修改当前广告为VALUE优化模式
                            # 出价控制
                            effect_adset = mongo.get_adset_id(effect_ad_id)
                            cur_bid = bsc.get_bid([effect_adset])[0][effect_adset]
                            new_bid = cur_bid + 2000 * coef
                            bsc.update_bid(new_bid, cur_bid, effect_adset, effect_ad_id)
                            mongo.blood_return(effect_ad_id, back_blood, 'prize')


def handler():
    sum_spend = 0
    sum_install = 0
    sum_pay = 0
    remove_no_spend_ads = 0
    pause_has_spend_ads = 0
    stop_campaigns = list()
    cmd_list = list()
    # 获取状态活跃的广告列表
    ads_dict = ds.snap_data.active_ads_dict
    relevance_score = ds.snap_data.active_ads_relevance_score
    relevance_gt6_count = 0
    for ad_id in relevance_score:
        if relevance_score[ad_id] >= 6:
            relevance_gt6_count = relevance_gt6_count + 1
    cur_index['gt6'] = relevance_gt6_count
    inactive_ads = ds.snap_data.inactive_ads_dict
    # 检查广告状态
    pending_lt4, pending_gte4 = status_check()
    # 获取这些广告的效果数据
    ad_indexes = ds.snap_data.active_ads_indexes
    # 更新snapshot表
    # cp_handler.listen(ad_indexes, cur_index['today'], tool.get_time_now())
    # 记录到insights表中
    mongo.record_insights(ad_indexes)
    # check血量
    blood_keys = deepcopy(list(bb.ads_group_blood.keys()))
    for blood_ad_id in blood_keys:
        if blood_ad_id not in ad_indexes:
            del bb.ads_group_blood[blood_ad_id]
            if blood_ad_id in bb.ads_group_index:
                del bb.ads_group_index[blood_ad_id]
            if blood_ad_id in bb.ads_acc_blood_change:
                del bb.ads_acc_blood_change[blood_ad_id]
    for ad_id in ad_indexes:
        # 设置广告blood
        bb.set_blood(ad_id, bb.calc_ad_blood(ad_id, ad_indexes))
        spend, install, pay = ad_indexes[ad_id]
        if ad_id in bb.ads_group_index:
            spend_old, install_old, pay_old = bb.ads_group_index[ad_id]
        else:
            spend_old, install_old, pay_old = 0, 0, 0
        # 更新ads_group_index
        bb.ads_group_index[ad_id] = ad_indexes[ad_id]
        # 如果血量为0，则关闭广告
        if bb.ads_group_blood[ad_id] == 0:
            stop_campaigns.append(ads_dict[ad_id][0])
            if spend > 0:
                cmd_list.append('PAUSED')
                pause_has_spend_ads = pause_has_spend_ads + 1
            else:
                cmd_list.append('DELETED')
                remove_no_spend_ads = remove_no_spend_ads + 1
        add_spend = (spend - spend_old)
        add_install = (install - install_old)
        add_pay = (pay - pay_old)
        if add_spend < 0:
            add_spend = spend
            add_install = install
            add_pay = pay
        sum_spend = sum_spend + add_spend
        sum_install = sum_install + add_install
        sum_pay = sum_pay + add_pay
    # stop ads by 100
    if len(stop_campaigns) > 0:
        log.logger.info("stop " + str(len(stop_campaigns)) + " campaigns for 0 blood")
        bsc.stop_campaign_process(stop_campaigns, cmd_list)
    # 获取该投放在线活跃广告数量，花费、安装、付费次数的数据
    cur_index['active'] = len(ads_dict)
    cur_index['pending'] = pending_lt4
    cur_index['paused'] = len(inactive_ads)
    mongo.update_delivery_data(cur_index['today'], sum_spend, sum_install, sum_pay, remove_no_spend_ads,
                               pause_has_spend_ads, cur_index['active'] + cur_index['pending'],
                               cur_index['paused'], 0, 0, pending_gte4, cur_index['gt6'])
