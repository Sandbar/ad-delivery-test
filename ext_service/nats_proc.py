import asyncio
import json
from nats.aio.client import Client as Nats
from common import utils as tool
import os
import common.logger as log


def json_compose(campaign_id, cmd_type):
    json_obj = {
        'token': os.environ["access_token"],
        'request': {
            'apiVersion': tool.FB_API_VERSION,
            'path': campaign_id,
            'body': 'status=' + cmd_type,  # DELETED,PAUSED
            'method': "POST"
        },
        'account': os.environ["account_id"],
        'priority': 1,
        'noRetry': True,
        'replyTo': os.environ["reply_to"],
        'payload': campaign_id
    }
    j = json.dumps(json_obj)
    return j.encode(encoding="utf-8")


async def run(loop, obj_ids, cmd_type):
    nc = Nats()
    await nc.connect(io_loop=loop, servers=[os.environ["nat_url"]])
    i = 0
    for req in obj_ids:
        await nc.publish("fb_api.async.request", json_compose(req, cmd_type[i]))
        i = i + 1
    await asyncio.sleep(20)  # 10秒够用了，还不知道怎么确定所有请求发送成功
    await nc.close()


def control_campaign(campaign_ids, cmd_type):
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(run(loop, campaign_ids, cmd_type))
    except Exception as e:
        log.log_exp()
        raise e
