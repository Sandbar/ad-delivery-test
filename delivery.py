import sys
import ext_service.mongo as mongo
import os
import resource
import time

resource.get_config()
pro_home = __file__[:__file__.rfind("/")]
pid_filename = os.path.join(pro_home, 'pid.log')
pid_file = open(pid_filename, 'a')
pid_file.write(str(os.getpid()))
pid_file.write('\n')
pid_file.flush()
pid_file.close()


def get_input():
    delivery_name = ''
    if len(sys.argv) == 2:
        delivery_name = sys.argv[1]

    if len(sys.argv) == 1:
        delivery_name = input("请输入投放名称：")
    return delivery_name


def env_init():
    out = mongo.get_delt_info(get_input())
    while len(out) == 0:
        print("输入有误~~~")
        out = mongo.get_delt_info(get_input())

    # 设置环境变量
    os.environ["account_id"] = str(out[0]['account_id'])
    os.environ["access_token"] = mongo.get_access_token(os.environ["account_id"])
    os.environ["country"] = out[0]['country']
    os.environ["platform"] = out[0]['platform']
    os.environ["flag"] = out[0]['flag']
    os.environ["name"] = out[0]['name']
    os.environ["language"] = out[0]['language']
    os.environ["init_ads_size"] = str(out[0]['init_ads_size'])
    os.environ["dead_size"] = str(out[0]['dead_size'])
    os.environ["bid_from"] = str(out[0]['bid_from'])
    os.environ["bid_to"] = str(out[0]['bid_to'])
    os.environ["act_id"] = 'act_' + os.environ["account_id"]

    log_dir = pro_home + '/logs'
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)

    os.environ["pro_home"] = pro_home
    os.environ["log_dir"] = pro_home + '/logs'
    os.environ["log_file"] = log_dir + '/' + out[0]['name'] + '.log'


def monitor():
    env_init()
    import blood.blood_control as bc
    import common.logger as log
    import common.utils as tool
    import data_center.snapshot as ds
    import blood.blood_base as bb
    import bid_rank.bid_control as bid
    cur_minutes = time.strftime('%M', time.localtime(time.time()))
    log.logger.info('cur_minutes:' + cur_minutes)
    while True:
        if time.strftime('%M', time.localtime(time.time())) == cur_minutes:
            log.logger.info("......meet......")
            bc.cur_index['today'] = tool.get_today()
            # 更新数据
            ds.snap_data.update(bc.cur_index['today'])
            # 更新控制系数、投放状态信息
            bb.update_args()
            if bb.delivery_status == "on":
                # 获取效果指标，处理血量，关闭血量为0的广告
                log.logger.info("//handler")
                bc.handler()
                # 检查广告数据，根据init_size创建一定数量的广告
                log.logger.info("//online_check")
                bc.online_check()
                log.logger.info("//dead_check")
                bc.dead_check()
                # 奖励回血+提高出价
                log.logger.info("//prize_control")
                bc.prize_control()
                # 周期回血
                log.logger.info("//period_control")
                bc.period_control()
                # 出价跟踪
                log.logger.info("//bid_trace")
                bid.bid_trace()
                log.logger.info("//waiting next period")
            else:
                log.logger.info("current delivery is off.")
            # 等待1分钟
            time.sleep(60)
        else:
            time.sleep(1)


if __name__ == "__main__":
    monitor()
