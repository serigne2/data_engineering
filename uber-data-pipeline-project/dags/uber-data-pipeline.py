from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) 
import pandas as pd
from pipelines.get_data_from_cstorage import extract_data_from_cloud_storage
from pipelines.transform import transform
from pipelines.load_bigquery import load_to_bigquery

default_args = {
    'start_date': datetime(2026, 1, 1)
}

dag = DAG(
    'uber_etl_xcom',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['uber_etl']
)

# ------------------ Tasks ------------------

def extract_task(**context):
    df = extract_data_from_cloud_storage()
    path = f"/opt/airflow/data/raw/uber-data-{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    print(f"Fichier créé : {path}, taille {os.path.getsize(path)} bytes")
    # Push le chemin dans XCom
    context['ti'].xcom_push(key='csv_path', value=path)
    return path 
    

extract = PythonOperator(
    task_id='extract',
    python_callable=extract_task,
    provide_context=True,
    dag=dag
)

def transform_task(**context):
    ti = context['ti']
    print("ALL XCOM:", ti.xcom_pull(task_ids='extract'))
    print("KEY csv_path:", ti.xcom_pull(task_ids='extract', key='csv_path'))
    path = ti.xcom_pull(task_ids='extract')
    #path = ti.xcom_pull(task_ids='extract', key='csv_path')
    print("PATH RECUPERE :", path)
    if path is None:
        raise ValueError("XCom n'a pas renvoyé le chemin du fichier")
    df = pd.read_csv(path)
    data_dict = transform(df)
    # Push le dict transformé
    ti.xcom_push(key='transformed_data', value=data_dict)

transform_op = PythonOperator(
    task_id='transform',
    python_callable=transform_task,
    provide_context=True,
    dag=dag
)

def load_task(**context):
    ti = context['ti']
    data_dict = ti.xcom_pull(task_ids='transform', key='transformed_data')
    load_to_bigquery(data_dict)
    print("Données chargées dans BigQuery.")

load_op = PythonOperator(
    task_id='load',
    python_callable=load_task,
    provide_context=True,
    dag=dag
)

# ------------------ Dependencies ------------------
extract >> transform_op >> load_op








# # from airflow import DAG
# # from datetime import datetime
# import sys
# import os
# #import io
# # from airflow.operators.python import PythonOperator
# sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) # à bien comprendre
# # from pipelines.get_data_from_cstorage import extract_data_from_cloud_storage
# from airflow.decorators import dag, task
# from datetime import datetime
# import pandas as pd

# from pipelines.get_data_from_cstorage import extract_data_from_cloud_storage
# from pipelines.transform import transform
# from pipelines.load_bigquery import load_to_bigquery

# @dag(
#     start_date=datetime(2024, 1, 1),
#     schedule=None,
#     catchup=False,
#     tags=["uber_etl"]
# )
# def uber_etl_taskflow():

#     @task
#     def extract():
#         df = extract_data_from_cloud_storage()
#         path = "/opt/airflow/data/raw/uber-data-{}.csv".format(datetime.now().strftime("%Y%m%d%H%M%S"))
#         # Crée le dossier si nécessaire
#         os.makedirs(os.path.dirname(path), exist_ok=True)
#         df.to_csv(path, index=False)
#         # Vérification rapide
#         if not os.path.exists(path):
#               raise FileNotFoundError(f"Fichier non trouvé : {path}")
#         print(f"Fichier créé avec succès : {path}, {os.path.getsize(path)} bytes")
#         return path

#     @task
#     def transform_task(data):
#         import pandas as pd
#         df = pd.read_csv(data)
#         result = transform(df)
#         return result

#     @task
#     def load(data_dict):
#         load_to_bigquery(data_dict)

#     raw_data = extract()
#     transformed_data = transform_task(raw_data)
#     load(transformed_data)


# uber_etl_taskflow()

