import requests
import json
import os


def gsm_message(message):
    at_users = os.environ["at_users"]
    data = {
        "msgtype": "text",
        "text": {
            "content": message
        },
        "at": {
            "atMobiles": at_users.split(','),
            "isAtAll": False
        }
    }
    req_out = requests.post(os.environ["access_url"], data=json.dumps(data),
                            headers={'Content-Type': "application/json"})
    json_out = json.loads(req_out.text)
    return json_out
