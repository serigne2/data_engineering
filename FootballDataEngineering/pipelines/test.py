#fichier wikipedia pipeline -------         ----------------------------------------
# import json

# import pandas as pd
# from geopy import Nominatim
# import os
# import time


# os.makedirs('data', exist_ok=True)

# NO_IMAGE = 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/No-image-available.png/480px-No-image-available.png'


# def get_wikipedia_page(url):
#     import requests

#     print("Getting wikipedia page...", url)
#     headers = {
#     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#                   "AppleWebKit/537.36 (KHTML, like Gecko) "
#                   "Chrome/120.0.0.0 Safari/537.36",
#     "Accept-Language": "en-US,en;q=0.9",
# }

#     try:
#         response = requests.get(url, headers=headers, timeout=10)
#         response.raise_for_status()  # check if the request is successful
#         #print("Wikipedia page retrieved successfully.")
#         return response.text
#     except requests.RequestException as e:
#         print(f"An error occured: {e}")


# def get_wikipedia_data(html):
#     import pandas as pd
#     from io import StringIO

#     tables = pd.read_html(StringIO(html))

#     if len(tables) < 2:
#         raise ValueError("Wikipedia tables not found or page structure changed")

#     df = tables[1]  # table principale des sta

#     return df


# def clean_text(text):
#     text = str(text).strip()
#     text = text.replace('&nbsp', '')
#     if text.find(' ♦'):
#         text = text.split(' ♦')[0]
#     if text.find('[') != -1:
#         text = text.split('[')[0]
#     if text.find(' (formerly)') != -1:
#         text = text.split(' (formerly)')[0]

#     return text.replace('\n', '')


# def extract_wikipedia_data(url, ti=None):
#     #url = kwargs['url']
#     html = get_wikipedia_page(url)  
#     rows = get_wikipedia_data(html)
#     df=rows
#     #print(rows)
#     #print(df.columns.tolist())

#     data = []
#     for i, row in df.iterrows():
#         values = {
#             'rank': int(row['Rank']) if 'Rank' in row else i + 1,
#             'stadium': clean_text(row['Stadium']),
#             'capacity': str(row['Seating capacity']).replace(',', '').replace('.', ''),
#             'region': clean_text(row['Region']),
#             'country': clean_text(row['Country']),
#             'city': clean_text(row['City']),
#             'images': NO_IMAGE,  # image pas toujours fiable via pandas
#             'home_team': clean_text(row['Home team(s)']),
#         }
#         data.append(values)

#     json_rows = json.dumps(data)

#     # Respect total de la logique Airflow
#     #kwargs['ti'].xcom_push(key='rows', value=json_rows)
#     if ti:
#         ti.xcom_push(key='rows', value=json_rows)

#     return json_rows



# #     data = []

# #     for i in range(1, len(rows)):
# #         tds = rows[i].find_all('td')
# #         values = {
# #             'rank': i,
# #             'stadium': clean_text(tds[0].text),
# #             'capacity': clean_text(tds[1].text).replace(',', '').replace('.', ''),
# #             'region': clean_text(tds[2].text),
# #             'country': clean_text(tds[3].text),
# #             'city': clean_text(tds[4].text),
# #             'images': 'https://' + tds[5].find('img').get('src').split("//")[1] if tds[5].find('img') else "NO_IMAGE",
# #             'home_team': clean_text(tds[6].text),
# #         }
# #         data.append(values)

# #     json_rows = json.dumps(data)
# #     print(json_rows)
# #     kwargs['ti'].xcom_push(key='rows', value=json_rows)

# #     return "OK"


# def get_lat_long(country, city):
#     geolocator = Nominatim(user_agent='football_data_engineering_project_2026')
#     location = geolocator.geocode(f'{city}, {country}')
    
#     time.sleep(1)  # Respect Nominatim policy
#     if location:
#         return location.latitude, location.longitude

#     return None


# def transform_wikipedia_data(ti=None):
#     #data = kwargs['ti'].xcom_pull(key='rows', task_ids='extract_data_from_wikipedia')
#     data = ti.xcom_pull(
#         key='rows',
#         task_ids='extract_data_from_wikipedia'
#     )
#     data = json.loads(data)

#     stadiums_df = pd.DataFrame(data)
#     stadiums_df['location'] = stadiums_df.apply(lambda x: get_lat_long(x['country'], x['stadium']), axis=1)
#     stadiums_df['images'] = stadiums_df['images'].apply(lambda x: x if x not in ['NO_IMAGE', '', None] else NO_IMAGE)
#     stadiums_df['capacity'] = stadiums_df['capacity'].astype(int)

#     # handle the duplicates
#     duplicates = stadiums_df[stadiums_df.duplicated(['location'])]
#     duplicates['location'] = duplicates.apply(lambda x: get_lat_long(x['country'], x['city']), axis=1)
#     stadiums_df.update(duplicates)

#     # push to xcom
#     #kwargs['ti'].xcom_push(key='rows', value=stadiums_df.to_json())
#     ti.xcom_push(
#         key='rows',
#         value=stadiums_df.to_json()
#     )

#     return "OK"


# def write_wikipedia_data(**kwargs):
#     from datetime import datetime
#     data = kwargs['ti'].xcom_pull(key='rows', task_ids='transform_wikipedia_data')

#     data = json.loads(data)
#     data = pd.DataFrame(data)

#     file_name = ('stadium_cleaned_' + str(datetime.now().date())
#                  + "_" + str(datetime.now().time()).replace(":", "_") + '.csv')

#     # data.to_csv('data/' + file_name, index=False)
#     data.to_csv('abfs://footballdataeng@footballdataeng.dfs.core.windows.net/data/' + file_name,
#                 storage_options={
#                     'account_key': 'pcrbWAsuPmzOH43lu1xang05pIs+g1Lys/bor0z59O38sVyWQNQ64AtEveMobZ2pIwCjqximReKY+ASt9dP/+A=='
#                 }, index=False)
# extract_wikipedia_data(url="https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity")

#fichier wikipedia flow -------         ----------------------------------------
# import os
# import sys
# from datetime import datetime

# from airflow import DAG
# from airflow.operators.python import PythonOperator

# sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# from pipelines.wikipedia_pipeline import extract_wikipedia_data, transform_wikipedia_data, write_wikipedia_data

# dag = DAG(
#     dag_id='wikipedia_flow',
#     default_args={
#         "owner": "Yusuf Ganiyu",
#         "start_date": datetime(2023, 10, 1),
#     },
#     schedule_interval=None,
#     catchup=False
# )

# extract_data_from_wikipedia = PythonOperator(
#     task_id="extract_data_from_wikipedia",
#     python_callable=extract_wikipedia_data,
#     #provide_context=True,
#     op_kwargs={"url": "https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"},
#     dag=dag
# )

# transform_wikipedia_data = PythonOperator(
#     task_id='transform_wikipedia_data',
#     provide_context=True,
#     python_callable=transform_wikipedia_data,
#     dag=dag
# )

# write_wikipedia_data = PythonOperator(
#     task_id='write_wikipedia_data',
#     provide_context=True,
#     python_callable=write_wikipedia_data,
#     dag=dag
# )

# extract_data_from_wikipedia >> transform_wikipedia_data >> write_wikipedia_data
#------------------        ------------------------
#uploader le fichier vers ma vm: scp data-engineering-lab-481110-e339fbfca94e.json macbookair@docker-vm:~/data_engineering/FootballDataEngineering/gcp/
