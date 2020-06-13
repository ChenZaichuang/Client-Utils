from .config import Config
from python_utils.logger import CustomLogger
from python_utils.misc import get_variable_from_local
import requests


class PlatformAPIClient:

    def __init__(self, env='uat'):
        self.platform_host = Config.get_config(env, "platform_host")
        self.okta_host = Config.get_config(env, "okta_host")
        self.token_path = Config.get_config(env, "token_path")
        self.okta_token = Config.get_config(env, "okta_token")
        self.logger = CustomLogger()

    def get_assignment_by_ass_id(self, ass_id_list):
        return self.get_platform_data("api ApiCommonReadAccess", "jigsaw/assignments", "ids[]=" + "&ids[]=".join(ass_id_list))

    def _format_jigsaw_date(self, date):
        return "-".join(date.split("-")[::-1])

    def get_staffing_requests_by_opportunity_id(self, opportunity_id):
        return self.get_platform_data("api", f"sales-system-service/opportunities/{opportunity_id}/staffing-requests")['content']

    def get_assignments_by_opportunity_id(self, opportunity_id):
        results = self.get_platform_data("api ApiCommonReadAccess", f"jigsaw/assignments", f"opportunity_ids[]={opportunity_id}")
        for res in results:
            res["duration"]["startsOn"] = self._format_jigsaw_date(res["duration"]["startsOn"])
            res["duration"]["endsOn"] = self._format_jigsaw_date(res["duration"]["endsOn"])
        return results

    def get_platform_data(self, scope, path, params=None):
        self.logger.info(f"get_platform_data with {path}")
        for _ in range(3):

            res = requests.get(f"{self.platform_host}/{path}",
                         params=params,
                         headers={'Authorization': f'bearer {self.get_token(scope)}'})

            if res.status_code != 200:
                self.logger.error(f"Failed to get {path}, res: {res.status_code} | {res.text}")
            else:
                return res.json()

        raise RuntimeError(f"failed to get_platform_data with {path}")

    def get_token(self, scope):
        return get_variable_from_local(f'platform_{scope}_token', function_value=self.refresh_token, args=(scope,), keep_time=3000)

    def refresh_token(self, scope):
        return requests.post(url=f"{self.okta_host}/oauth2/{self.token_path}/v1/token",
                                   data={'grant_type': 'client_credentials', 'scope': scope},
                                   headers={
                                       'Authorization': f'Basic {self.okta_token}',
                                       'Content-Type': 'application/x-www-form-urlencoded'}
                                   ).json()['access_token']

    def get_opportunity_by_id(self, opportunity_id):
        return self.get_platform_data("api", f"sales-system-service/opportunities/{opportunity_id}")
