import json
from gevent import sleep
from python_utils.logger import Logger
from request_client import NewRequest
from .config import Config


def request_with_retry(func, args=None, kwargs=None, retries=3, retry_interval=5):
    res = None
    for _ in range(retries):
        if args is None:
            args = tuple()
        if kwargs is None:
            kwargs = dict()
        res = func(*args, **kwargs)
        if res.status_code != 200:
            sleep(retry_interval)
        else:
            break
    else:
        raise RuntimeError(f"failed to execute {func.__name__} with args={str(args)}, kwargs={str(kwargs)}: {res.status_code} | {res.text}")


class ForcetalkClient:

    def __init__(self, env='uat'):
        self.forcetalk_host = Config.get_config(env, "forcetalk_host")

    def delete_resource_request(self, res_req_id):
        forcetalk_url = f'{self.forcetalk_host}/forcetalk/ResourceRequest/{res_req_id}'
        headers = {"Accept": "application/json", "Content-type": "application/json"}
        Logger.debug(f"delete_resource_request with {res_req_id}")
        request_with_retry(NewRequest.delete, kwargs=dict(url=forcetalk_url, headers=headers, timeout=30))

    def send_staffing_request_to_forcetalk(self, staffing_request):
        Logger.debug(f"send_staffing_request_to_forcetalk with {staffing_request}")
        data = {
            "id": staffing_request['id'],
            "project": {
                'sf_id': staffing_request['opportunityId']
            },
            "startDate": staffing_request['startDate'],
            "endDate": staffing_request['endDate'],
            "probability": staffing_request['probability'],
            'effort': staffing_request['effort'],
            "grade": staffing_request['gradeName'],
            "role": staffing_request['roleName'],
            "workingOffices": self.generate_working_offices(staffing_request['workingOffices'])
        }
        forcetalk_url = f'{self.forcetalk_host}/forcetalk/ResourceRequest?checkEligible=false'
        headers = {"Accept": "application/json", "Content-type": "application/json"}
        request_with_retry(NewRequest.post, kwargs=dict(url=forcetalk_url, headers=headers, data=json.dumps(data), timeout=30))

    def generate_working_offices(self, working_offices):
        return [{
            'name': office['officeName'],
            'startDate': office['startDate'],
            'endDate': office['endDate'],
            'rate': float(office['rate']),
        } for office in working_offices]

    def flag_as_daily_rate_project(self, opportunity_id):
        Logger.debug(f"flag_as_daily_rate_project with {opportunity_id}")
        forcetalk_url = f'{self.forcetalk_host}/forcetalk/Project/flagAsDailyRateProject/{opportunity_id}'
        headers = {"Accept": "application/json", "Content-type": "application/json"}
        request_with_retry(NewRequest.put, kwargs=dict(url=forcetalk_url, headers=headers, timeout=30))

    def delete_assignment(self, ass_id):
        Logger.debug(f"delete_assignment with {ass_id}")
        forcetalk_url = f'{self.forcetalk_host}/forcetalk/Assignment/{ass_id}'
        headers = {"Accept": "application/json", "Content-type": "application/json"}
        request_with_retry(NewRequest.put, kwargs=dict(url=forcetalk_url, headers=headers, timeout=30))

    def send_assignment_to_forcetalk(self, assignment):
        Logger.debug(f"send_assignment_to_forcetalk with {assignment}")
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
        request_with_retry(NewRequest.post, kwargs=dict(url=forcetalk_url, headers=headers, data=json.dumps(data), timeout=30))

    def flag_project_as_eligible_for_live_feed(self, opportunity_id):
        Logger.debug(f"flagProjectAsEligibleForLiveFeed with {opportunity_id}")
        forcetalk_url = f'{self.forcetalk_host}/forcetalk/Project/flagForLiveFeed/{opportunity_id}'
        headers = {"Accept": "application/json", "Content-type": "application/json"}
        request_with_retry(NewRequest.put, kwargs=dict(url=forcetalk_url, headers=headers, timeout=30))
