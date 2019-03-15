import requests
import json
import os
import common.logger as log
import common.utils as tool


def get_config(count):
    return {
            "count": count,
            "deliveryName": os.environ["name"],
            "token": os.environ["access_token"],
            "accountId": os.environ["account_id"]
    }


def create_new_ads(count):
    data = get_config(count)
    req_out = requests.post(os.environ["gen_ads_service"],
                            data=json.dumps(data), headers=tool.HEADERS, timeout=1200)
    log.logger.info("count:" + str(count) + " " + req_out.text)
    json_obj = json.loads(req_out.text)
    return json_obj['n']
