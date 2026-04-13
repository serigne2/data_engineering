import pandas as pd
import io 
import os
from google.cloud import storage
def extract_data_from_cloud_storage():
    client = storage.Client()
    bucket = client.get_bucket('uber-data-serigne_project')
    blob = bucket.blob('uber_data.csv')
    data = blob.download_as_text()
    df= pd.read_csv(io.StringIO(data), sep=',')
    #os.makedirs('data/raw', exist_ok=True)
    #df.to_csv('data/raw/uber-data.csv', index=False)    
    return df

