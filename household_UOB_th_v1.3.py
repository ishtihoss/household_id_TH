from pyspark import SparkContext, SparkConf, HiveContext
import pyspark.sql.functions as F
import pyspark.sql.types as T
from math import radians, cos, sin, asin, sqrt
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType
import geohash2 as geohash
import pyspark.sql.window as W


n_path = 's3a://ada-dev/ishti/input_th_uob/uob_th_input (1).csv'
ndf = spark.read.format('csv').option('header','true').load(n_path)

# Create geohash function

def latlong_geohash(latitude, longitude):
    geohash_6 = geohash.encode(latitude,longitude, precision=9)
    return geohash_6

latitude = ndf['lat'].cast('float')
longitude = ndf['lon'].cast('float')

ghash = F.udf(latlong_geohash, StringType())

ndf = ndf.withColumn('geohash9',ghash(latitude,longitude))
ndf = ndf.withColumnRenamed('geohash9','home_geohash')

# ingest home location database

ho_path = 's3a://ada-prod-data/etl/data/brq/sub/home-office/home-office-data/TH/202012/home-location/'
ho_df = spark.read.parquet(ho_path)

# inner join on geohash

joined_df1 = ho_df.join(ndf, on='home_geohash', how='inner')

# ingest household id

hid_path = "s3a://ada-dev/Ampi/household/TH/household_id/restrict_ip/smc_custom1/2021-04-01_2021-04-30/*"
hid_df  = spark.read.parquet(hid_path)


# join with household_id part 1

joined_df2 = hid_df.join(joined_df1,how='inner', on='ifa')

# collect unique sets

c_sets = joined_df2.groupBy('household_id').agg(F.collect_set('home_geohash').alias('home_geohash'),F.collect_set('ifa').alias('ifa'))

# Additional information from household id dataset

add_df_path = 's3a://ada-dev/Ampi/household/TH/household_id/restrict_ip/smc_custom/2021-04-01_2021-04-30/'
add_df = spark.read.parquet(add_df_path)

# Final joining

df_x = add_df.join(c_sets, on='household_id', how='inner')

# Write file

output = "s3a://ada-dev/ishti/houehold_UOB_updated_pure/"
df_x.write.format("parquet").option("compression", "snappy").save(output)

# Part II (Code snippet)

def strip(st):
    if not st:
        return 'None'
    else:
        return st[:6]




s_u = F.udf(strip,T.StringType())




df = spark.read.parquet("s3a://ada-dev/ishti/houehold_UOB_updated_pure/")
df = df.where('household_size<15')
df = df.withColumn('geohas',F.explode('home_geohash'))
df = df.withColumn('geohash',s_u('geohas')).drop('geohas')
# df1 =df.withColumn('data',F.struct('household_id','household_size'))
# df1 = df1.groupBy('geohash').agg(F.collect_set('data').alias('data'))



w = W.Window.partitionBy("geohash")
df = df.withColumn("avg_size", F.mean('household_size').over(w))
dfs = df.groupBy('geohash').agg(F.stddev('household_size').alias('stddev'))
df = df.join(dfs,'geohash','inner')
df = df.withColumn('deviation',df.household_size-df.avg_size)



df1 = df.groupBy('geohash').agg(F.countDistinct('household_id').alias('household_count'))
df = df.join(df1,'geohash','inner')
df1 = df.where('household_count>1')



df1 = df1.withColumn('single_dev',F.when(df1.deviation > df1.stddev,1).otherwise(0))
df1 = df1.withColumn('double_dev',F.when(df1.deviation > 2*df1.stddev,1).otherwise(0))
df1 = df1.withColumn('triple_dev',F.when(df1.deviation > 3*df1.stddev,1).otherwise(0))

#Output processed File

output = "s3a://ada-dev/ishti/houehold_UOB_updated_processed/"
df1.write.format("parquet").option("compression", "snappy").save(output)
