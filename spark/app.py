from pyspark.sql import SparkSession
import os
import multiprocessing
import redis
import time
import json
import datetime

client = redis.Redis(host='redis', port=6379)
brand_count_for_date = dict()

def update_to_redis(row):
    data_to_update = {
        "id": row['id'],
        "brand": row['brand'],
        "color": row['colors'],
        "date": row['dateAdded']
    }
    product_created_date = convert_str_to_date_object(row['dateAdded'])
    date = product_created_date.strftime("%Y-%m-%d")
    score = product_created_date.timestamp()
    
    update_product_count_for_date(row, date)
    client.zadd(row['colors'], {json.dumps(data_to_update) : int(score)})
    client.zadd(date, {json.dumps(data_to_update) : int(score)})

def update_product_count_for_date(row, product_created_date):
    dict_key = product_created_date+row['brand']
    if dict_key in brand_count_for_date.keys():
        value = brand_count_for_date[dict_key]
        score = value + 1
        brand_count_for_date[dict_key] += 1
        client.zadd(product_created_date+'_count', {row['brand'] : score})
    else:
        brand_count_for_date[dict_key] = 1
        client.zadd(product_created_date+'_count', {row['brand'] : 1})

def convert_str_to_date_object(date_string):
    return datetime.datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%SZ")

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
    for row in pool.map(update_to_redis, data_without_null.rdd.collect()):
        pass
        
    duration = time.time() - start_time
    print(f"Inserted in {duration} seconds")
