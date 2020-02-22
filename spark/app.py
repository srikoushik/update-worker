from pyspark.sql import SparkSession
import os
import multiprocessing
import redis
import time

client = redis.Redis(host='localhost', port=6379)

def updateToRedis(row):
    client.set(row['id'], row['colors'])

if __name__ == '__main__':
    scSpark = SparkSession \
        .builder \
        .appName("Reading csv") \
        .getOrCreate()

    file_data = os.path.dirname(os.path.realpath(__file__)) + "/dataset/*.csv"

    df = scSpark.read.csv(file_data, header=True).cache()

    transformed_data = df.select("id", "dateAdded", "colors", "brand")

    data_without_null = transformed_data.where("id is not null and dateAdded is not null and colors is not null and brand is not null")
    
    start_time = time.time()

    pool = multiprocessing.Pool(processes = multiprocessing.cpu_count()-1)
    for row in pool.map(updateToRedis, data_without_null.rdd.collect()):
        pass
        
    duration = time.time() - start_time
    print(f"Inserted in {duration} seconds")
