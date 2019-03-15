import requests
import threading
import time
import json
import common.utils as tool
import os
import common.logger as log


class BatchProcess:
    def __init__(self, data_dict):
        self.data_dict = data_dict
        self.result = dict()
        self.tasks = dict()
        self.pool_size = tool.THREAD_POOL_SIZE

    def work(self, dict_key):
        while True:
            try:
                req_out = requests.post(os.environ["facebook_service"], data=self.data_dict[dict_key], headers=tool.HEADERS)
                self.result[dict_key] = json.loads(req_out.text)
                break
            except Exception as e:
                log.logger.info(str(e))
                log.log_exp()
                time.sleep(20)

    def run(self):
        for dict_key in self.data_dict:
            if len(self.tasks) < self.pool_size:
                self.new_thread(dict_key)
            else:
                self.wait_one()
                self.new_thread(dict_key)
        self.wait_end()
        return self.result

    def wait_end(self):
        while len(self.tasks) > 0:
            self.update()
            time.sleep(0.1)

    def update(self):
        keys = list(self.tasks.keys())
        for task_key in keys:
            if not self.tasks[task_key].isAlive():
                del self.tasks[task_key]

    def wait_one(self):
        while len(self.tasks) == self.pool_size:
            self.update()
            time.sleep(0.1)

    def new_thread(self, dict_key):
        task = threading.Thread(target=self.work, args=(dict_key,))
        self.tasks[dict_key] = task
        task.start()


def compose(prefix, since=tool.get_today(), until=tool.get_today(), set_time_range=True):
    time_range = "&time_range={since:'" + since + "',until:'" + until + "'}"
    if not set_time_range:
        time_range = ""
    body = {
        'token': os.environ["access_token"],
        'request': {
            'apiVersion': tool.FB_API_VERSION,
            'path': prefix + time_range
        },
        'account': os.environ["account_id"],
        'priority': 1,
        'noRetry': True
    }
    return json.dumps(body)


def compose_ads(ad_id, fields):
    return compose(ad_id + "?fields=" + fields)


def compose_insights(ad_id, fields, since=tool.get_today(), until=tool.get_today()):
    return compose(ad_id+"/insights?fields="+fields, since, until)


def get_relevance_score(json_data):
    if len(json_data) == 0:
        return 0
    elif json_data[0]['relevance_score']['status'] == 'OK':
        return int(json_data[0]['relevance_score']['score'])
    else:
        return 0


def get_ads_relevance(ads_id):
    ads_data = {x: compose_insights(x, 'relevance_score', since=tool.get_past_date(10)) for x in ads_id}
    bp_obj = BatchProcess(ads_data)
    out = bp_obj.run()
    result = dict()
    for key in ads_data:
        if 'body' in out[key]:
            if 'data' in out[key]['body']:
                result[key] = get_relevance_score(out[key]['body']['data'])
            else:
                result[key] = 0
        else:
            result[key] = 0
    return result


def get_ads_status(ads_id):
    ads_data = {x: compose_ads(x, 'effective_status') for x in ads_id}
    bp_obj = BatchProcess(ads_data)
    out = bp_obj.run()
    result = dict()
    for key in out:
        if 'effective_status' in out[key]['body']:
            result[key] = out[key]['body']['effective_status']
    return result


def get_ads_insights(ads_id, since=tool.get_today(), until=tool.get_today()):
    ads_data = {x: compose_insights(x, 'spend,actions', since, until) for x in ads_id}
    bp_obj = BatchProcess(ads_data)
    out = bp_obj.run()
    result = dict()
    for key in out:
        tuple_tmp = (0, 0, 0)
        if 'body' in out[key]:
            if 'data' in out[key]['body']:
                if len(out[key]['body']['data']) != 0:
                    tuple_tmp = tool.get_insights_by_json(out[key]['body'])
        result[key] = tuple_tmp
    return result


def get_adset_bid_amount(adsets_id):
    adset_data = {x: compose(x + "?fields=bid_amount", set_time_range=False) for x in adsets_id}
    bp_obj = BatchProcess(adset_data)
    out = bp_obj.run()
    result = dict()
    none_id = list()
    for key in out:
        if 'body' in out[key]:
            if 'bid_amount' in out[key]['body']:
                result[key] = out[key]['body']['bid_amount']
            else:
                log.logger.info(str(key)+" adset bidamount is none-2.")
                none_id.append(key)
        else:
            log.logger.info(str(key) + " adset bidamount is none-1.")
            none_id.append(key)
    if len(none_id) == 0:
        return result
    else:
        log.logger.info("get adset bid amount again......")
        new_out = get_adset_bid_amount(none_id)
        return dict(result, **new_out)
