import pandas as pd
from pandas.io import gbq
from google.cloud import bigquery

def Data_Load(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
         
         
    """
    file_name = event['name']
    table_name = file_name.split('.')[0]
    file_format = file_name.split('.')[1]
    
    # Actual file data , writing to Big Query
    def jsonLoad():
         df_data = pd.read_json('gs://' + event['bucket'] + '/' + file_name)

         df_data.to_gbq('POC.' + table_name, 
                        project_id='gcp-poc1-381608', 
                        if_exists='append',
                        location='us')

    def csvLoad():
          df_data = pd.read_csv('gs://' + event['bucket'] + '/' + file_name)

          df_data.to_gbq('POC.' + table_name, 
                        project_id='gcp-poc1-381608', 
                        if_exists='append',
                        location='us')
    
    if file_format=='csv':
          csvLoad()
    else:
          jsonLoad()
    
   
