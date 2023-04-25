from google.oauth2 import service_account
from google.cloud import bigquery
import gspread
from gspread_dataframe import set_with_dataframe

# Set up authentication credentials
creds = service_account.Credentials.from_service_account_file('creds.json')
client = bigquery.Client(credentials=creds)
gc = gspread.service_account(filename='creds.json')

# Define BigQuery project and dataset ID
project_id = 'gcp-poc1-381608'
dataset_id = 'POC'

# Define BigQuery table ID
table_id = 'employees'

# Query BigQuery table and store results in a Pandas DataFrame
query = f'SELECT * FROM `{project_id}.{dataset_id}.{table_id}`'
df = client.query(query).to_dataframe()
print(df)

# Open a new or existing Google Sheet and add the DataFrame as a new worksheet
sheet_name = 'Sales_data'
tab_name='Sheet1'
sh = gc.open(sheet_name)
worksheet = sh.worksheet(tab_name)
set_with_dataframe(worksheet,df)
