from google.cloud import bigquery
import pandas as pd

def load_to_bigquery(data_dict):

    # client = bigquery.Client.from_service_account_json(
    #     '/home/macbookair/data_engineering/uber-data-pipeline-project/gcp/data-engineering-lab-481110-2e3ee70ceb82.json'
    # )
    client = bigquery.Client()

    dataset_id = "uber_dataset"

    for table_name, table_data in data_dict.items():

        df = pd.DataFrame.from_dict(table_data)

        table_id = f"{client.project}.{dataset_id}.{table_name}"

        job = client.load_table_from_dataframe(df, table_id)
        job.result()

        print(f"Loaded {table_name} into BigQuery")