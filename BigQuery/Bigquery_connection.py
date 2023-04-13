import os
from google.cloud import bigquery
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "gcp-poc1-381608-274159d7407c.json"
client = bigquery.Client()
sql_query= """
select * from gcp-poc1-381608.view.s limit 100
"""
query_job = client.query(sql_query)

for row in query_job.result():
    print(row)
