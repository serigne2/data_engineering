import json

import pandas as pd
from geopy import Nominatim
import os
import time


os.makedirs('data', exist_ok=True)

NO_IMAGE = 'https://upload.wikimedia.org/wikipedia/commons/thumb/0/0a/No-image-available.png/480px-No-image-available.png'


def get_wikipedia_page(url):
    import requests

    print("Getting wikipedia page...", url)
    headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()  # check if the request is successful
        #print("Wikipedia page retrieved successfully.")
        return response.text
    except requests.RequestException as e:
        print(f"An error occured: {e}")


def get_wikipedia_data(html):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table", class_="wikitable")

    main_table = None

    for table in tables:
        headers = [th.text.strip() for th in table.find_all("th")]
        if "Seating capacity" in headers:
            main_table = table
            break


    rows = main_table.find_all("tr")
    #print(rows)
    return rows


def clean_text(text):
    text = str(text).strip()
    text = text.replace('*', '')
    text = text.replace('&nbsp', '')
    if text.find(' ♦'):
        text = text.split(' ♦')[0]
    if text.find('[') != -1:
        text = text.split('[')[0]
    if text.find(' (formerly)') != -1:
        text = text.split(' (formerly)')[0]

    return text.replace('\n', '')


def extract_wikipedia_data(url, ti=None):
    #url = kwargs['url']
    html = get_wikipedia_page(url)  
    rows = get_wikipedia_data(html)
    df=rows
    #print(rows)
    #print(df.columns.tolist())

    data = []
    for i in range(1, len(rows)):
        tds = rows[i].find_all('td')
        values = {
            'rank': i,
            'stadium': clean_text(tds[0].text),
            'capacity': clean_text(tds[1].text).replace(',', '').replace('.', ''),
            'region': clean_text(tds[2].text),
            'country': clean_text(tds[3].text),
            'city': clean_text(tds[4].text),
            'images': 'https://' + tds[5].find('img').get('src').split("//")[1] if tds[5].find('img') else "NO_IMAGE",
            'home_team': clean_text(tds[6].text),
        }
        data.append(values)

    json_rows = json.dumps(data)
    #data_df = pd.DataFrame(data)
    #data_df.to_csv('data/wikipedia_football_stadiums.csv', index=False)
    #print(data_df.head())

    
    if ti:
       ti.xcom_push(key='rows', value=json_rows)

    return json_rows


def get_lat_long(country, city):
    geolocator = Nominatim(user_agent='football_data_engineering_project_2026')
    location = geolocator.geocode(f'{city}, {country}')
    
    time.sleep(1)  # Respect Nominatim policy
    if location:
        return location.latitude, location.longitude

    return None


def transform_wikipedia_data(ti=None):
    #data = kwargs['ti'].xcom_pull(key='rows', task_ids='extract_data_from_wikipedia')
    data = ti.xcom_pull(
        key='rows',
        task_ids='extract_data_from_wikipedia'
    )
    data = json.loads(data)

    stadiums_df = pd.DataFrame(data)
    stadiums_df['location'] = stadiums_df.apply(lambda x: get_lat_long(x['country'], x['stadium']), axis=1)
    stadiums_df['images'] = stadiums_df['images'].apply(lambda x: x if x not in ['NO_IMAGE', '', None] else NO_IMAGE)
    stadiums_df['capacity'] = stadiums_df['capacity'].astype(int)

    # handle the duplicates
    duplicates = stadiums_df[stadiums_df.duplicated(['location'])]
    duplicates['location'] = duplicates.apply(lambda x: get_lat_long(x['country'], x['city']), axis=1)
    stadiums_df.update(duplicates)

    # push to xcom
    #kwargs['ti'].xcom_push(key='rows', value=stadiums_df.to_json())
    ti.xcom_push(
        key='rows',
        value=stadiums_df.to_json()
    )

    return "OK"


def write_wikipedia_data(**kwargs):
    from datetime import datetime
    from io import StringIO
    from google.cloud import storage
    from io import StringIO

    data = kwargs['ti'].xcom_pull(key='rows', task_ids='transform_wikipedia_data')

    data = json.loads(data)
    data = pd.DataFrame(data)

    file_name = ('stadium_cleaned_' + str(datetime.now().date())
                  + "_" + str(datetime.now().time()).replace(":", "_") + '.csv')
    
    bucket_name = "football-data-palaye"
    blob_path = f"processed/{file_name}"
    # 3️ Conversion DataFrame → CSV en mémoire
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)

    # 4️ Upload vers GCS
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    blob.upload_from_string(
        csv_buffer.getvalue(),
        content_type="text/csv"
    )

    print(f" File uploaded to gs://{bucket_name}/{blob_path}")
#write_wikipedia_data()
