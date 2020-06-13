import copy
import random
from collections import OrderedDict

import requests
from gevent import sleep
from gevent.libev.corecext import traceback
from requests.adapters import HTTPAdapter
from simple_salesforce import Salesforce

from python_utils.logger import CustomLogger
from python_utils.thread_pool import ThreadPool
from .config import Config


class SalesforceClient:

    def __init__(self, env='uat'):
        max_connections = 300
        session = requests.Session()
        adapter = HTTPAdapter(pool_connections=max_connections, pool_maxsize=max_connections, max_retries=3)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        self.sf = Salesforce(**{**Config.get_config(env, "sf_oauth"), "session": session})
        self.pool = ThreadPool(total_thread_number=50)
        self.logger = CustomLogger()

    def query_records(self, query_string, include_deleted=False, extract_fields_list=None, extract_fields_set=None, extract_field_map=None, extract_fields_list_map=None, extract_fields_set_map=None):
        while True:
            try:
                page_size = 2000
                result = self.sf.query(query_string, include_deleted=include_deleted)
                all_records = result['records']
                if not result["done"]:
                    total_size = int(result["totalSize"])
                    page_url = result['nextRecordsUrl'].split("-")[0] + "-%s"
                    pool = self.pool.new_pool_status()
                    for page_num in range(page_size, total_size, page_size):
                        pool.apply_async(self.sf.query_more, (page_url % page_num, dict(identifier_is_url=True)))
                    for result in pool.get_results_order_by_index():
                        all_records.extend(result["records"])
                if extract_fields_list is not None:
                    return {field_name: [record[field_name] for record in all_records] for field_name in extract_fields_list}
                elif extract_fields_set is not None:
                    return {field_name: {record[field_name] for record in all_records} for field_name in extract_fields_set}
                elif extract_field_map is not None:
                    assert type(extract_field_map) is tuple
                    key_field_name, value_field_name = extract_field_map
                    return {record[key_field_name]: record[value_field_name] for record in all_records}
                elif extract_fields_list_map is not None:
                    assert type(extract_field_map) is tuple
                    key_field_name, value_field_name = extract_fields_list_map
                    m = dict()
                    for record in all_records:
                        m[key_field_name] = m.get(key_field_name, []) + [record[value_field_name]]
                    return m
                elif extract_fields_set_map is not None:
                    assert type(extract_field_map) is tuple
                    key_field_name, value_field_name = extract_fields_set_map
                    m = dict()
                    for record in all_records:
                        m[key_field_name] = m.get(key_field_name, set()) | {record[value_field_name]}
                    return m
                else:
                    return all_records
            except:
                err_msg = traceback.format_exc()
                if 'UNABLE_TO_LOCK_ROW' not in err_msg and 'ConcurrentPerOrgLongTxn Limit exceeded' not in err_msg:
                    raise RuntimeError(err_msg)
                sleep(random.randint(2, 20))

    def bulk_DML_records(self, object_api_name, operation, data):
        assert operation in ('insert', 'update', 'delete')
        data = list(data)
        res = getattr(getattr(self.sf.bulk, object_api_name), operation)(data)
        for index, result in enumerate(res):
            if not result['success']:
                self.logger.error(f'delete failed {data[index]} | {str(result)}')

    def _dml_record(self, object_api_name, operation, data):
        assert operation in ('insert', 'update', 'delete')
        while True:
            try:
                copied_data = copy.deepcopy(data)
                if 'insert' == operation:
                    res = getattr(self.sf, object_api_name).create(copied_data)
                    self.logger.info(f'{object_api_name} {operation}: {res}')
                    return res['id']
                elif 'update' == operation:
                    rec_id = copied_data['Id']
                    del copied_data['Id']
                    res = getattr(self.sf, object_api_name).update(rec_id, copied_data)
                    self.logger.info(f'{object_api_name} {operation}: {res}')
                    assert int(res) < 400
                    return res
                else:
                    return getattr(self.sf, object_api_name).delete(copied_data['Id'])
            except :
                err_msg = traceback.format_exc()
                print('err_msg: ' + err_msg)
                if 'UNABLE_TO_LOCK_ROW' not in err_msg and 'ConcurrentPerOrgLongTxn Limit exceeded' not in err_msg:
                    raise RuntimeError(err_msg)
                sleep(random.randint(2, 20))

    def dml_records(self, object_api_name, operation, data):
        assert operation in ('insert', 'update', 'delete')
        assert type(data) is dict or type(data) is list or type(data) is OrderedDict
        self.logger.info(f'{object_api_name} {operation}: {data}')
        if type(data) is dict or type(data) is OrderedDict:
            return self._dml_record(object_api_name, operation, data)
        elif len(data) > 0:
            pool = self.pool.new_pool_status()
            for record in data:
                pool.apply_async(self._dml_record, (object_api_name, operation, record))
            pool.get_results_order_by_index(raise_exception=True)

    def get_all_active_projects(self):
        return self.query_records("select Id, pse__Opportunity__c, pse__Is_Billable__c from pse__Proj__c where Project_Code_FF__c != null and pse__Is_Active__c = true and pse__Closed_for_Time_Entry__c = false and pse__Closed_for_Expense_Entry__c = false and pse__Opportunity__r.pse__Primary_Project__c != null and (not (pse__Project_Type__c = 'Internal' and pse__Region__r.Name = 'ThoughtWorks' and pse__Allow_Timecards_Without_Assignment__c = true))")

    def get_res_req_by_proj_id(self, proj_id):
        return self.query_records(f"select Jigsaw_ID__c, pse__Start_Date__c, pse__End_Date__c, pse__Resource_Role__c, Custom_Resource_Role__c, (select Start_Date__c, End_Date__c, Bill_Rate__c, Region__r.Name from Resource_Request_Working_Offices__r) from pse__Resource_Request__c where pse__Project__c = '{proj_id}'")

    def get_ass_by_proj_id(self, proj_id):
        return self.query_records(f"select Id, Jigsaw_Assignment_ID__c, pse__Start_Date__c, pse__End_Date__c, Resource_Request__r.Jigsaw_ID__c, pse__Percent_Allocated__c, Shadow__c, pse__Bill_Rate__c from pse__Assignment__c where pse__Project__c = '{proj_id}' and pse__Status__c != 'Closed'")

    def get_projects_by_oppo_or_proj_ids(self, oppo_ids=None, proj_ids=None):
        assert oppo_ids is not None or proj_ids is not None
        pool = self.pool.new_pool_status()
        if oppo_ids is not None:
            for id in oppo_ids:
                pool.apply_async(self.query_records, (f"select Id, pse__Opportunity__c, pse__Is_Billable__c from pse__Proj__c where Project_Code_FF__c != null and pse__Is_Active__c = true and pse__Closed_for_Time_Entry__c = false and pse__Closed_for_Expense_Entry__c = false and pse__Opportunity__c = '{id}' and pse__Opportunity__r.pse__Primary_Project__c != null",))
        else:
            for id in proj_ids:
                pool.apply_async(self.query_records, (f"select Id, pse__Opportunity__c, pse__Is_Billable__c from pse__Proj__c where Project_Code_FF__c != null and pse__Is_Active__c = true and pse__Closed_for_Time_Entry__c = false and pse__Closed_for_Expense_Entry__c = false and Id = '{id}' and pse__Opportunity__r.pse__Primary_Project__c != null",))
        return [res[0] for res in pool.get_threads_result() if len(res) > 0]

    def get_all_employee_ids(self):
        return {employee["Employee_ID__c"] for employee in self.query_records("select Employee_ID__c from Contact where RecordType.Name = 'Resource' and pse__Is_Resource__c = true and pse__Is_Resource_Active__c = true")}

    def get_all_employees(self):
        return self.query_records("select Id, pse__Start_Date__c, Department__c from Contact where RecordType.Name = 'Resource' and pse__Start_Date__c != null")
