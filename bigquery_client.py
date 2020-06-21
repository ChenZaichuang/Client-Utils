import os

from google.cloud import bigquery

from python_utils.logger import Logger
from python_utils.thread_pool import ThreadPool
from .config import Config


class BigqueryClient:

    def __init__(self, env='uat'):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = f'client_utils/{Config.get_config(env, "GOOGLE_APPLICATION_CREDENTIALS")}'

    def load_csv_file_into_bigquery(self, dataset_id, table_id, file_name, schema):
        client = bigquery.Client()

        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.skip_leading_rows = 0
        job_config.write_disposition = 'WRITE_TRUNCATE'
        job_config.autodetect = True
        job_config.schema = schema

        with open(file_name, "rb") as source_file:
            job = client.load_table_from_file(
                source_file,
                table_ref,
                location="US",
                job_config=job_config,
            )
        job.result()
        Logger.debug(f"op=load-csv-file-into-bigquery | status=OK | desc=Loaded {job.output_rows} rows into {dataset_id}:{table_id}")

    def query_rows_from_bigquery(self, query_string):
        client = bigquery.Client()
        return list(client.query(query_string).result())

    def write_rows_to_bigquery(self, dataset_id, table_id, rows_to_insert):
        client = bigquery.Client()
        table_ref = client.dataset(dataset_id).table(table_id)
        table = client.get_table(table_ref)
        errors = client.insert_rows(table, rows_to_insert)
        if errors:
            raise RuntimeError(f"op=write_rows_to_bigquery | status=OK| desc=dataset_id: {dataset_id}, table_id: {table_id}, rows_to_insert: {str(rows_to_insert)}, errors: {str(errors)}")
        else:
            Logger.debug(f"op=write_rows_to_bigquery | status=OK | desc=dataset_id: {dataset_id}, table_id: {table_id}, rows_to_insert: {str(rows_to_insert)}")

    def create_dataset(self, dataset_id):
        client = bigquery.Client()
        client.create_dataset(dataset_id, exists_ok=True)

    def create_connection(self, client, dataset_id, table_id):
        dataset_ref = client.dataset(dataset_id)
        dataset = client.get_dataset(bigquery.Dataset(dataset_ref))
        return dataset.table(table_id)

    def create_table(self, dataset_id, table_id, schema):
        client = bigquery.Client()
        table_ref = self.create_connection(client, dataset_id, table_id)
        table = bigquery.Table(table_ref, schema)
        client.create_table(table)

    def delete_tables_in_datasets(self, dataset_ids):
        thread_pool = ThreadPool(total_thread_number=30)
        client = bigquery.Client()
        thread_results = []
        for dataset_id in dataset_ids:
            tables = list(client.list_tables(dataset_id))
            for table in tables:
                thread_results.append(thread_pool.apply_async(self.delete_table, (dataset_id, table.table_id,)))
        thread_pool.get_results_order_by_index()

    def delete_table(self, dataset_id, table_id):
        client = bigquery.Client()
        table_ref = self.create_connection(client, dataset_id, table_id)
        table = bigquery.Table(table_ref)
        client.delete_table(table, not_found_ok=True)
