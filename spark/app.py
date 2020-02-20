from pyspark.sql import SparkSession
import os

if __name__ == '__main__':
    scSpark = SparkSession \
        .builder \
        .appName("Reading csv") \
        .getOrCreate()

data_file = os.path.dirname(os.path.realpath(__file__)) + "/dataset/*.csv"

sdfData = scSpark.read.csv(data_file, header=True, sep=",").cache()

# Take record which has fields (id, brand, color, dateAdded)
print('Total Records = {}'.format(sdfData.count()))

# sdfData.show()