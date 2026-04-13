from time import sleep

import pyspark
import openai
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType


def start_streaming(spark):
    #topic = 'customers_review'
    #while True:
        try:
            stream_df = (spark.readStream.format("socket")
                         .option("host", "spark-master")
                         .option("port", 9999)
                         .load()
                         )

            schema = StructType([
                StructField("review_id", StringType()),
                StructField("user_id", StringType()),
                StructField("business_id", StringType()),
                StructField("stars", FloatType()),
                StructField("date", StringType()),
                StructField("text", StringType())
            ])

            stream_df = stream_df.select(from_json(col('value'), schema).alias("data")).select(("data.*"))
            query = (stream_df.writeStream.outputMode("append").format("console").option("truncate", False).start())
            query.awaitTermination()

            
        except Exception as e:
            print(f'Exception encountered: {e}. Retrying in 10 seconds')
            sleep(10)

if __name__ == "__main__":
    spark_conn = SparkSession.builder.appName("SocketStreamConsumer").getOrCreate()

    start_streaming(spark_conn)
