from collections import OrderedDict
import copy
import functools
import traceback

from gevent import Timeout, sleep
from gevent.timeout import Timeout as TimeoutException
import requests
from requests.adapters import HTTPAdapter
from simple_salesforce import Salesforce

from python_utils.logger import Logger
from python_utils.thread_pool import ThreadPool
from .config import Config


def salesforce_timeout_retry(timeout=30, retries=5, retry_interval=3):
    def _decorate(function):
        @functools.wraps(function)
        def wrapped_function(*args, **kwargs):
            for _ in range(retries):
                try:
                    with Timeout(timeout):
                        res = function(*args, **kwargs)
                        if res is not None:
                            return res
                except TimeoutException:
                    sleep(retry_interval)
                except Exception as e:
                    err_msg = traceback.format_exc()
                    if 'UNABLE_TO_LOCK_ROW' not in err_msg and 'ConcurrentPerOrgLongTxn Limit exceeded' not in err_msg:
                        raise e
            raise RuntimeError(f"Failed after {retries} times")
        return wrapped_function
    return _decorate


class SalesforceClient:

    def __init__(self, env='uat'):
        max_connections = 10
        session = requests.Session()
        adapter = HTTPAdapter(pool_connections=max_connections, pool_maxsize=max_connections, max_retries=3)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        self.sf = Salesforce(**{**Config.get_config(env, "sf_oauth"), "session": session})
        self.pool = ThreadPool(total_thread_number=10)

    @salesforce_timeout_retry()
    def query_with_timeout(self, query_string):
        return self.sf.query(query_string, include_deleted=False)

    @salesforce_timeout_retry()
    def query_more_with_timeout(self, next_records_identifier):
        return self.sf.query_more(next_records_identifier, identifier_is_url=True)

    def query_all_fast(self, query_string):
        page_size = 2000
        result = self.query_with_timeout(query_string)
        all_records = result['records']
        if not result["done"]:
            total_size = int(result["totalSize"])
            page_url = result['nextRecordsUrl'].split("-")[0] + "-%s"
            pool = self.pool.new_shared_pool()
            for page_num in range(page_size, total_size, page_size):
                pool.apply_async(self.query_more_with_timeout, (page_url % page_num,))
            for result in pool.get_results_order_by_index():
                all_records.extend(result["records"])
        assert all_records is not None, query_string
        return all_records

    def query_all_stable(self, query_string):
        all_records = []
        result = self.query_with_timeout(query_string)
        while True:
            all_records.extend(result['records'])
            if not result['done']:
                result = self.query_more_with_timeout(result['nextRecordsUrl'], identifier_is_url=True)
            else:
                break
        result['records'] = all_records
        return result

    def bulk_DML_records(self, object_api_name, operation, data):
        assert operation in ('insert', 'update', 'delete')
        data = list(data)
        res = getattr(getattr(self.sf.bulk, object_api_name), operation)(data)
        for index, result in enumerate(res):
            if not result['success']:
                Logger.error(f'delete failed {data[index]} | {str(result)}')

    @salesforce_timeout_retry()
    def _dml_record(self, object_api_name, operation, data):
        assert operation in ('insert', 'update', 'delete')
        copied_data = copy.deepcopy(data)
        if 'insert' == operation:
            res = getattr(self.sf, object_api_name).create(copied_data)
            Logger.debug(f'{object_api_name} {operation}: {res}')
            return res['id']
        elif 'update' == operation:
            rec_id = copied_data['Id']
            del copied_data['Id']
            res = getattr(self.sf, object_api_name).update(rec_id, copied_data)
            Logger.debug(f'{object_api_name} {operation}: {res}')
            assert int(res) < 400
            return res
        else:
            return getattr(self.sf, object_api_name).delete(copied_data['Id'])


    def dml_records(self, object_api_name, operation, data):
        assert operation in ('insert', 'update', 'delete')
        assert type(data) is dict or type(data) is list or type(data) is OrderedDict
        Logger.debug(f'{object_api_name} {operation}: {data}')
        if type(data) is dict or type(data) is OrderedDict:
            return self._dml_record(object_api_name, operation, data)
        elif len(data) > 0:
            pool = self.pool.new_shared_pool()
            for record in data:
                pool.apply_async(self._dml_record, (object_api_name, operation, record))
            pool.get_results_order_by_index(raise_exception=True)

    def get_all_active_projects(self):
        return self.query_all_fast("select Id, pse__Opportunity__c, pse__Is_Billable__c from pse__Proj__c where Project_Code_FF__c != null and pse__Is_Active__c = true and pse__Closed_for_Time_Entry__c = false and pse__Closed_for_Expense_Entry__c = false and pse__Opportunity__r.pse__Primary_Project__c != null and (not (pse__Project_Type__c = 'Internal' and pse__Region__r.Name = 'ThoughtWorks' and pse__Allow_Timecards_Without_Assignment__c = true))")

    def get_res_req_by_proj_id(self, proj_id):
        return self.query_all_fast(f"select Jigsaw_ID__c, pse__Start_Date__c, pse__End_Date__c, pse__Resource_Role__c, Custom_Resource_Role__c, (select Start_Date__c, End_Date__c, Bill_Rate__c, Region__r.Name from Resource_Request_Working_Offices__r) from pse__Resource_Request__c where pse__Project__c = '{proj_id}'")

    def get_ass_by_proj_id(self, proj_id):
        return self.query_all_fast(f"select Id, Jigsaw_Assignment_ID__c, pse__Start_Date__c, pse__End_Date__c, Resource_Request__r.Jigsaw_ID__c, pse__Percent_Allocated__c, Shadow__c, pse__Bill_Rate__c from pse__Assignment__c where pse__Project__c = '{proj_id}' and pse__Status__c != 'Closed'")

    def get_projects_by_oppo_or_proj_ids(self, oppo_ids=None, proj_ids=None):
        assert oppo_ids is not None or proj_ids is not None
        pool = self.pool.new_shared_pool()
        if oppo_ids is not None:
            for id in oppo_ids:
                pool.apply_async(self.query_all_fast, (f"select Id, pse__Opportunity__c, pse__Is_Billable__c from pse__Proj__c where Project_Code_FF__c != null and pse__Is_Active__c = true and pse__Closed_for_Time_Entry__c = false and pse__Closed_for_Expense_Entry__c = false and pse__Opportunity__c = '{id}' and pse__Opportunity__r.pse__Primary_Project__c != null",))
        else:
            for id in proj_ids:
                pool.apply_async(self.query_all_fast, (f"select Id, pse__Opportunity__c, pse__Is_Billable__c from pse__Proj__c where Project_Code_FF__c != null and pse__Is_Active__c = true and pse__Closed_for_Time_Entry__c = false and pse__Closed_for_Expense_Entry__c = false and Id = '{id}' and pse__Opportunity__r.pse__Primary_Project__c != null",))
        return [res[0] for res in pool.get_results_order_by_index() if len(res) > 0]

    def get_all_employee_ids(self):
        return {employee["Employee_ID__c"] for employee in self.query_all_fast("select Employee_ID__c from Contact where RecordType.Name = 'Resource' and pse__Is_Resource__c = true and pse__Is_Resource_Active__c = true")}

    def get_all_employees(self):
        return self.query_all_fast("select Id, pse__Start_Date__c, Department__c from Contact where RecordType.Name = 'Resource' and pse__Start_Date__c != null")
