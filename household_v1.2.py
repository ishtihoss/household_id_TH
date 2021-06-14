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

# Home office Data

ho_path = 's3a://ada-prod-data/etl/data/brq/sub/home-office/home-office-data/TH/202012/home-location/'
ho_df = spark.read.parquet(ho_path)

# Natasha's dataset for fullview geonfence

nat_path = 's3a://ada-dev/Natassha/202106/uob_refinance/output_geofence_2/fullview-geofence/'
nat_df = spark.read.format("csv").option("header","true").load(nat_path)
nat_df = nat_df.withColumnRenamed('dev_ifa','ifa')

# Join level 1 (nat_df with home office df)

df_l1 = ho_df.join(nat_df, on='ifa', how='inner')

# join level 2 (df_l1 with household_id df)

dfl_2 = df_l1.join(df, on='ifa', how='inner')


# Group by household ID and collect all sets of home geohashes and ifas

porky = dfl_2.groupBy('household_id').agg(F.collect_set('home_geohash').alias('home_geohash'),F.collect_set('ifa').alias('ifa'))

# Additional information from household id dataset

path2 = 's3a://ada-dev/Ampi/household/TH/household_id/restrict_ip/smc_custom/2021-04-01_2021-04-30/'
df2 = spark.read.parquet(path2)

# Final joining

df_x = df2.join(porky, on='household_id', how='inner')

# Write file

output = "s3a://ada-dev/ishti/houehold_UOB_updated/"
df_x.write.format("parquet").option("compression", "snappy").save(output)
