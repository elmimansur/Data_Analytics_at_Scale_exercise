import pyspark
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StructField,IntegerType,Row
from load_countries import loadCountries

spark = SparkSession.builder.appName('my_app').getOrCreate()
df = spark.read.json("../lectures/data/twitter-geo/2016-01-*.gz").select("text")

countries=["United States","United Kingdom","Indonesia"]
#countries=loadCountries()

def country_counts(row):
    counts=np.zeros(len(countries),dtype="int")
    for i in range(len(countries)):
        if countries[i] in row["text"]:
            counts[i]+=1
    return counts


results= df.rdd.map(country_counts).sum()


print(results[0:10])

spark.stop()
