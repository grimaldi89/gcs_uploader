import os
from dotenv import load_dotenv
from datetime import datetime, date, timezone, timedelta
import json
load_dotenv()


import bigframes.pandas
class BigFrameFrameExporter:
    def __init__(self,project=None, bpd=None):
        self.bpd = bpd or bigframes.pandas
        self.project = project or os.getenv("GCP_PROJECT")
        self.bpd.options.bigquery.project = self.project


    def _run_query(self,query):
        try:
            df = self.bpd.read_gbq(query)
            return df
        except ValueError as error:
            print(error)

    def _to_gcs(self,df,bucket,file_name):
        try:
            full_bucket_path = f"gs://{bucket}/{file_name}"
            df.to_parquet(full_bucket_path, compression="snappy", index=False)
        except ValueError as error:
            print(error)

    def _query(self,project,dataset,table, refresh_window, date_partition= None, sharded=True):
        date_now = datetime.now().date()
        query_list = []

        for day in range(1,refresh_window + 1):
            filter_date = date_now - timedelta(days=day)
            if not sharded:

                query = f'''
                
                select * from {project}.{dataset}.{table}
                where {date_partition} = '{filter_date}'
                
                '''
                query_list.append(query)
                print(query)

            else:
                filter_date = filter_date.strftime("%Y%m%d")
                query = f'''

                        select * from {project}.{dataset}.{table}_{filter_date}
                    
                        '''
                query_list.append(query)

        return query_list


    def export_to_gcs(self,table_list):
        if table_list is None:
            print("No table list")
        else:
            date_now = datetime.now().date()
            run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            for tl in table_list:
                project = tl["project"]
                dataset = tl["dataset"]
                for t in tl["tables"]:
                    refresh_window = t["refresh_window"]
                    date_partition_field = t["date_partition_field"]
                    sharded = t["sharded"]
                    table = t["table_name"]
                    queries = self._query(project=project,dataset=dataset,table=table,refresh_window=refresh_window,date_partition=date_partition_field,sharded=sharded)
                    for q in queries:
                        df = self._run_query(q)
                        self._to_gcs(df=df,bucket="parquet_files_test",file_name=f"{table}_{date_now}_*.parquet")










query = '''
select * from bigquery-public-data.usa_names.usa_1910_2013 limit 10
'''
#path = "gs://parquet_files_test/test/parquet_TESTE_*.parquet"
client = BigFrameFrameExporter()
#client._query(project="homelab-466922",dataset="analysis", table= "sample",refresh_window=8, date_partition="date",sharded=False)
with open("tables.json", "r") as j:
    tables = json.load(j)
    client.export_to_gcs(table_list=tables)
#df = client._run_query(query)
#client._to_gcs(df,bucket_path=path)


