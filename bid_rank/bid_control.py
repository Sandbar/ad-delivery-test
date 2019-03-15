import data_center.snapshot as ds
import ext_service.mongo as mongo
import data_center.data_service as bsc
import os

lvl1_min = 8000
lvl1_max = 11000
lvl2_min = 11500
lvl2_max = 16000
lvl3_min = 16500
lvl3_max = 20000
bid_step = 500
blood_args = mongo.get_blood_func_args_by_delivery(os.environ["name"])
cpi_min = blood_args['h_func']['from']
cpp_min = blood_args['p_func']['from']/2


def get_bid_level(bid_amount):
    if lvl1_min <= bid_amount < lvl1_max:
        return 'L1'
    if lvl2_min <= bid_amount < lvl2_max:
        return 'L2'
    if lvl3_min <= bid_amount < lvl3_max:
        return 'L3'
    return 'None'


def level3_manager(ad_id):
    spend, install, pay = ds.snap_data.active_ads_indexes[ad_id]
    if spend > 0:
        cur_spend, cur_install, cur_pay = mongo.get_bid_trace_tuple(ad_id)
        if pay - cur_pay >= 5 and (spend - cur_spend)/(pay - cur_pay) <= cpp_min:
            cur_bid = ds.snap_data.active_ads_bid_amount[ad_id]
            new_bid = cur_bid + bid_step
            if new_bid >= lvl3_max:
                new_bid = lvl3_max - 1
            bsc.update_bid_amount(ds.snap_data.active_ads_dict[ad_id][1], new_bid)
            # 追加跟踪信息
            mongo.record_bid_trace(ad_id, spend, install, pay, new_bid)


def level2_manager(ad_id):
    spend, install, pay = ds.snap_data.active_ads_indexes[ad_id]
    if spend > 0:
        cur_spend, cur_install, cur_pay = mongo.get_bid_trace_tuple(ad_id)
        delta_install = install - cur_install
        new_bid = 0
        if pay >= 5 and (spend/pay) <= cpp_min:
            # 满足条件，进阶出价
            new_bid = lvl3_min
        elif delta_install >= 10 and (spend - cur_spend)/delta_install <= cpi_min:
            # 调整出价
            cur_bid = ds.snap_data.active_ads_bid_amount[ad_id]
            new_bid = cur_bid + bid_step
            if new_bid >= lvl2_max:
                new_bid = lvl2_max - 1
        if new_bid > 0:
            bsc.update_bid_amount(ds.snap_data.active_ads_dict[ad_id][1], new_bid)
            # 追加跟踪信息
            mongo.record_bid_trace(ad_id, spend, install, pay, new_bid)


def level1_manager(ad_id):
    spend, install, pay = ds.snap_data.active_ads_indexes[ad_id]
    if spend > 0:
        cur_spend, cur_install, cur_pay = mongo.get_bid_trace_tuple(ad_id)
        delta_spend = spend - cur_spend
        if delta_spend > 0:
            if install >= 10 and spend/install <= cpi_min:
                # 满足条件，进阶出价
                new_bid = lvl2_min
            else:
                # 调整出价
                cur_bid = ds.snap_data.active_ads_bid_amount[ad_id]
                new_bid = cur_bid + int(delta_spend*100)
                if new_bid >= lvl1_max:
                    new_bid = lvl1_max - 1
            bsc.update_bid_amount(ds.snap_data.active_ads_dict[ad_id][1], new_bid)
            # 追加跟踪信息
            mongo.record_bid_trace(ad_id, spend, install, pay, new_bid)


def bid_trace():
    for key in ds.snap_data.active_ads_bid_amount:
        bid_level = get_bid_level(ds.snap_data.active_ads_bid_amount[key])
        if bid_level == "L1":
            level1_manager(key)
        if bid_level == "L2":
            level2_manager(key)
        if bid_level == "L3":
            level3_manager(key)
        #TODO:实现VALUE优化方式
