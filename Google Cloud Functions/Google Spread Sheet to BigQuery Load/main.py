import gspread 
import pandas as pd
from pandas.io import gbq
from google.cloud import bigquery

gc = gspread.service_account(filename='creds.json')
#sh = gc.open_by_key('1APvCyQww0UszHMXzkV9DL0Wiw8Aczsr26px3sShQDo0')
worksheet = gc.open('Sales_data').sheet1 # replace 'Sheet1' with your sheet name

#with open('output.csv', 'w', newline='') as file:
#    writer = csv.writer(file)
data = worksheet.get_all_records()
df = pd.DataFrame(data)
print(df)
df.to_gbq('POC.' +"Sales_data" , 
                        project_id='gcp-poc1-381608', 
                        if_exists='append',
                        location='us')
    
