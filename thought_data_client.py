from .config import Config
from python_utils.logger import Logger
from gevent import sleep
import requests


class ThoughtDataClient:

    def __init__(self, env='uat'):
        self.platform_host = Config.get_config(env, "thought_data_host")
        self.x_api_key = Config.get_config(env, "thought_data_x_api_key")

    def check_paused_subscriptions(self, subscription_name_list=None):
        for _ in range(3):
            try:
                res = requests.get(f"{self.platform_host}/v1/subscription", headers={"x-api-key": self.x_api_key})
            except:
                Logger.error(f'failed to get_subscription_by_name: {subscription_name_list}')
                sleep(5)
            else:
                if res.status_code == 200:
                    err_msg = ''
                    for subscription in res.json():
                        if subscription_name_list is None or subscription["name"] in subscription_name_list:
                            if subscription["state"] != "active":
                                err_msg += f'\n{subscription["name"]} is {subscription["state"]}\n'
                    if err_msg:
                        Logger.error(err_msg)
                    return err_msg
                else:
                    err_msg = f'failed to get_subscriptions: {res.status_code} | {res.text}'
                    Logger.error(err_msg)
                    raise RuntimeError(err_msg)
