import apache_beam as beam
import time
import csv
from io import StringIO

from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.options.pipeline_options import WorkerOptions

def parse_csv(line, columns):
    reader = csv.reader(StringIO(line))
    values = next(reader)
    return dict(zip(columns, values))

def run():
    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'data-engineering-lab-481110'
    google_cloud_options.region = 'us-central1'
    google_cloud_options.job_name = f'stadium-gcs-to-bq-{int(time.time())}'
    google_cloud_options.temp_location = 'gs://football-data-palaye/temp'
    google_cloud_options.service_account_email = 'airflow-gcs-writer@data-engineering-lab-481110.iam.gserviceaccount.com'
    google_cloud_options.staging_location = 'gs://football-data-palaye/staging'
    options.view_as(StandardOptions).runner = 'DataflowRunner'
    
    # --- Options workers pour réduire la taille et éviter les stockouts ---
    worker_options = options.view_as(WorkerOptions)
    worker_options.worker_zone = 'us-central1-a'  # ✅ c’est ici qu’on définit la zone
    #ajouter cette ligne data flow runner ne trouve pas la zone.Il choist la zo dispo
    worker_options.num_workers = 1               # nombre de workers minimum
    worker_options.max_num_workers = 2           # facultatif : maximum de workers
    worker_options.machine_type = 'n1-standard-1'  # machine type plus petite

    columns = ['rank','stadium','capacity','region','country','city','images','home_team','location' ]


    with beam.Pipeline(options=options) as p:
        (p
         | 'Read CSV' >> beam.io.ReadFromText(
                #'gs://football-data-palaye/processed/stadium_cleaned_2026-02-16_16_45_30.csv',
                'gs://football-data-palaye/processed/stadium_cleaned_2026-02-16_20_07_04.334126.csv',
                skip_header_lines=1)
         #| 'Parse CSV' >> beam.Map(lambda line: dict(zip(columns, line.split(','))))
         | 'Parse CSV' >> beam.Map(parse_csv, columns)

         | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                table='stadiums_cleaned',
                dataset='football',
                project='data-engineering-lab-481110',
                schema='rank:INTEGER,stadium:STRING,capacity:INTEGER,region:STRING,country:STRING,city:STRING,images:STRING,home_team:STRING,location:STRING',
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
            )
        )

if __name__ == '__main__':
    run()
