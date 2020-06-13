from config import Config
from python_utils.logger import CustomLogger
import requests
from gevent import sleep


class ThoughtDataClient:

    def __init__(self, env='uat'):
        self.platform_host = Config.get_config(env, "thought_data_host")
        self.x_api_key = Config.get_config(env, "thought_data_x_api_key")
        self.logger = CustomLogger()

    def is_subscription_active(self, subscription_name):
        for _ in range(3):
            try:
                res = requests.get(f"{self.platform_host}/v1/subscription", headers={"x_api_key": self.x_api_key})
            except:
                self.logger.error(f'failed to get_subscription_by_name: {subscription_name}')
                sleep(5)
            else:
                if res.status_code == 200:
                    for subscription in res.json():
                        if subscription["name"] == subscription_name:
                            return subscription["state"] == "active"
                    raise RuntimeError(f"subscription {subscription_name} not found")
                else:
                    err_msg = f'failed to get_subscription_by_name: {subscription_name} | {res.status_code} | {res.text}'
                    self.logger.error(err_msg)
                    raise RuntimeError(err_msg)
