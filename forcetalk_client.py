import json

from python_utils.logger import CustomLogger
from .config import Config
import requests
from gevent import sleep


class ForcetalkClient:

    def __init__(self, env='uat'):
        self.forcetalk_host = Config.get_config(env, "forcetalk_host")
        self.logger = CustomLogger()

    def delete_resource_request(self, res_req_id):
        forcetalk_url = f'{self.forcetalk_host}/forcetalk/ResourceRequest/{res_req_id}'
        headers = {"Accept": "application/json", "Content-type": "application/json"}
        self.logger.info(f"delete_resource_request with {res_req_id}")
        res = None
        for _ in range(3):
            res = requests.delete(url=forcetalk_url, headers=headers)
            if res.status_code != 200:
                sleep(5)
            else:
                break
        else:
            raise RuntimeError(f"failed to delete_resource_request with {res_req_id}: {res.status_code} | {res.text}")

    def send_staffing_request_to_forcetalk(self, staffing_request):
        self.logger.info(f"send_staffing_request_to_forcetalk with {staffing_request}")
        data = {
            "id": staffing_request['uuid'],
            "project": {
                'sf_id': staffing_request['project']['opportunityId']
            },
            "startDate": staffing_request['startDate'],
            "endDate": staffing_request['endDate'],
            "probability": staffing_request['probability'],
            'effort': staffing_request['effort'],
            "grade": staffing_request['grade']['name'],
            "role": staffing_request['role']['name'],
            "workingOffices": self.generate_working_offices(staffing_request['workInOffices'])
        }
        forcetalk_url = f'{self.forcetalk_host}/forcetalk/ResourceRequest?checkEligible=false'
        headers = {"Accept": "application/json", "Content-type": "application/json"}

        res = None
        for _ in range(3):
            res = requests.post(url=forcetalk_url, headers=headers, data=json.dumps(data))
            if res.status_code != 200:
                sleep(3)
            else:
                break
        else:
            raise RuntimeError(f"failed to delete_resource_request with {staffing_request}: {res.status_code} | {res.text}")

    def generate_working_offices(self, working_offices):
        return [{
            'name': office['name'],
            'startDate': office['startDate'],
            'endDate': office['endDate'],
            'rate': float(office['rate']),
        } for office in working_offices]

    def flag_as_daily_rate_project(self, opportunity_id):
        self.logger.info(f"flag_as_daily_rate_project with {opportunity_id}")

        forcetalk_url = f'{self.forcetalk_host}/forcetalk/Project/flagAsDailyRateProject/{opportunity_id}'
        headers = {"Accept": "application/json", "Content-type": "application/json"}

        res = None
        for _ in range(3):
            res = requests.put(url=forcetalk_url, headers=headers)
            if res.status_code != 200:
                sleep(3)
            else:
                break
        else:
            raise RuntimeError(f"failed to flag_as_daily_rate_project with {opportunity_id}: {res.status_code} | {res.text}")

    def delete_assignment(self, ass_id):
        self.logger.info(f"delete_assignment with {ass_id}")

        forcetalk_url = f'{self.forcetalk_host}/forcetalk/Assignment/{ass_id}'
        headers = {"Accept": "application/json", "Content-type": "application/json"}

        res = None
        for _ in range(3):
            res = requests.put(url=forcetalk_url, headers=headers)
            if res.status_code != 200:
                sleep(3)
            else:
                break
        else:
            raise RuntimeError(f"failed to delete_assignment with {ass_id}: {res.status_code} | {res.text}")

    def send_assignment_to_forcetalk(self, assignment):
        self.logger.info(f"send_assignment_to_forcetalk with {assignment}")
        data = {
            "id": assignment['id'],
            "project": {
                'sf_id': assignment['project']['opportunityId']
            },
            "staffingRequest": {
                "id": assignment['staffingRequest']['uuid']
            },
            "consultant": {
                'id': assignment['consultant']['employeeId']
            },
            "startDate": assignment['duration']['startsOn'],
            "endDate": assignment['duration']['endsOn'],
            "effort": int(assignment['effort']),
            "shadow": assignment['shadow'] == 'true',
        }
        forcetalk_url = f'{self.forcetalk_host}/forcetalk/Assignment?checkEligible=false'
        headers = {"Accept": "application/json", "Content-type": "application/json"}

        res = None
        for _ in range(3):
            res = requests.post(url=forcetalk_url, headers=headers, data=json.dumps(data))
            if res.status_code != 200:
                sleep(3)
            else:
                break
        else:
            raise RuntimeError(f"failed to send_assignment_to_forcetalk with {assignment}: {res.status_code} | {res.text}")

    def flag_project_as_eligible_for_live_feed(self, opportunity_id):
        self.logger.info(f"flagProjectAsEligibleForLiveFeed with {opportunity_id}")

        forcetalk_url = f'{self.forcetalk_host}/forcetalk/Project/flagForLiveFeed/{opportunity_id}'
        headers = {"Accept": "application/json", "Content-type": "application/json"}

        res = None
        for _ in range(3):
            res = requests.put(url=forcetalk_url, headers=headers)
            if res.status_code != 200:
                sleep(3)
            else:
                break
        else:
            raise RuntimeError(f"failed to flagProjectAsEligibleForLiveFeed with {opportunity_id}: {res.status_code} | {res.text}")