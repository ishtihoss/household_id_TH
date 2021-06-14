from pyspark import SparkContext, SparkConf, HiveContext
import pyspark.sql.functions as F
import pyspark.sql.types as T
from math import radians, cos, sin, asin, sqrt
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType
import geohash2 as geohash

# Houehold ID data
path = "s3a://ada-dev/Ampi/household/TH/household_id/restrict_ip/smc_custom1/2021-04-01_2021-04-30/*"

df = spark.read.parquet(path)

df.take(10)

# Fullview geonfence data

nat_path = 's3a://ada-dev/Natassha/202106/uob_refinance/output_geofence_2/fullview-geofence/'
nat_df = spark.read.format("csv").option("header","true").load(nat_path)

nat_df.take(10)

nat_df = nat_df.withColumnRenamed('dev_ifa','ifa')

#Inner joining two data frames

df_joined = nat_df.join(df, on='ifa', how='inner')

# Creaeting UDF

def latlong_geohash(latitude, longitude):
    geohash_6 = geohash.encode(latitude,longitude, precision=6)
    return geohash_6

latitude = df_joined['dev_lat'].cast('float')
longitude = df_joined['dev_lon'].cast('float')

ghash = F.udf(latlong_geohash, StringType())

# Adding geohash 6 column into the dataframe

df_c = df_joined.withColumn('geohash6',ghash(latitude,longitude))

# Write file

output = "s3a://ada-dev/ishti/houehold_UOB/"
df_c.write.format("parquet").option("compression", "snappy").save(output)
