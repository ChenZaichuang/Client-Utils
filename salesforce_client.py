from python_utils.thread_pool import ThreadPool
from python_utils.logger import CustomLogger

from collections import OrderedDict
import copy
import random
import time

from gevent.libev.corecext import traceback
import requests
from requests.adapters import HTTPAdapter
from simple_salesforce import Salesforce

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
                time.sleep(random.randint(2, 20))

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
                time.sleep(random.randint(2, 20))

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
