import os
import ext_service.mongo as mongo
from scipy import stats
import math
import common.logger as log

blood_control_coef = 1
delivery_status = "on"
blood_args = mongo.get_blood_func_args_by_delivery(os.environ["name"])
stats_trigger_date = ''
x_stats = stats.norm(loc=0, scale=0.025)
y_stats = stats.norm(loc=5, scale=15)
z_stats = stats.norm(loc=5, scale=10)
install_stats = stats.norm(loc=50, scale=50)
cpi_stats = stats.norm(loc=5, scale=3)
roi_stats = stats.norm(loc=0.05, scale=0.1)
pay_stats = stats.norm(loc=3, scale=3)
ads_group_index = dict()
ads_group_blood = dict()
ads_acc_blood_change = dict()


def update_args():
    out = mongo.get_delt_info(os.environ["name"])
    global blood_control_coef, delivery_status
    blood_control_coef = float(out[0]['control_coef'])
    delivery_status = out[0]['status']
    log.logger.info("update args blood_control_coef(" +
                    str(blood_control_coef) + ") delivery_status(" + delivery_status+")")


def blood(spend, install, pay, acc_spend, acc_pay):
    # 计算血量,True,False表示是否需要计算累积效应
    if spend > 0:
        if install > 0:
            if pay > 0:
                return p_func(acc_spend / acc_pay) * blood_control_coef, False
            else:
                return h_func(spend / install) * blood_control_coef, False
        else:
            return -spend * blood_control_coef, False
    else:
        return -10 * blood_control_coef, True


def h_func(cpi):
    v6 = blood_args['h_func']['from']
    v8 = blood_args['h_func']['to']
    v10 = blood_args['h_func']['max']
    if cpi <= v6:
        return v10
    if v6 < cpi <= v8:
        return v10 + (v10 / (v8 - v6))*(v6 - cpi)
    if cpi > v8:
        return (v8 - cpi)*10/math.sqrt(v8)


def p_func(acc_cpp):
    v200 = blood_args['p_func']['from']
    v400 = blood_args['p_func']['to']
    v20 = blood_args['p_func']['max']
    if acc_cpp <= v200:
        return v20
    else:
        value = (v20*v400)/(v400 - v200)
        return value - (v20/(v400 - v200)) * acc_cpp


def cal_roi(sum_install, sum_spend):
    if sum_install > 0 and sum_spend > 1:
        return 0.03 * sum_install * 10 / sum_spend
    else:
        return 0


def filter_by_insights(ads_xgs, n):
    ads_score = dict()
    for ad_id in ads_xgs:
        spend, install, pay = ads_xgs[ad_id]
        if install == 0:
            cpi = 50
        else:
            cpi = spend / install
        score = ((1 - cpi_stats.cdf(cpi)) * install_stats.cdf(install) * pay_stats.cdf(pay)) ** (1 / 3)
        ads_score[score] = ad_id
    # 选出广告
    select_ads = dict()
    if len(ads_score) > n:
        tmp = [(k, ads_score[k]) for k in sorted(ads_score.keys(), reverse=True)][0:n]
        for value in tmp:
            select_ads[value[1]] = value[0]
    else:
        for key in ads_score:
            select_ads[ads_score[key]] = key
    return select_ads


def calc_ad_blood(ad_id, ad_indexes):
    spend, install, pay = ad_indexes[ad_id]
    if ad_id not in ads_group_index:
        blood_val, is_acc = blood(spend, install, pay, spend, pay)
    else:
        spend_old, install_old, pay_old = ads_group_index[ad_id]
        if spend < spend_old:
            blood_val, is_acc = blood(spend, install, pay, spend, pay)
        else:
            blood_val, is_acc = blood(spend - spend_old, install - spend_old, pay - pay_old, spend, pay)
    if is_acc:
        if ad_id not in ads_acc_blood_change:
            ads_acc_blood_change[ad_id] = 1
        else:
            ads_acc_blood_change[ad_id] = ads_acc_blood_change[ad_id] + 1
        blood_val = blood_val * math.pow(ads_acc_blood_change[ad_id], 0.7)
    else:
        ads_acc_blood_change[ad_id] = 0
    return blood_val


def set_blood(ad_id, blood_val, default=100):
    if ad_id not in ads_group_blood:
        ads_group_blood[ad_id] = default
    ads_group_blood[ad_id] = round(ads_group_blood[ad_id] + blood_val, 2)
    if ads_group_blood[ad_id] < 0:
        ads_group_blood[ad_id] = 0
    if ads_group_blood[ad_id] > 100:
        ads_group_blood[ad_id] = 100
