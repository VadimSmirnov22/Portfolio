import findspark
import sys 
import os
findspark.init()
findspark.find()

import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from datetime import datetime
from pyspark.sql.functions import to_timestamp

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'


def zones_slicing(geo_events_source: str, geo_cities: str, spark: SparkSession) -> DataFrame:

    events = spark.read.parquet(geo_events_source)
    events_day = events.filter(F.col("lat").isNotNull() & F.col("lat").isNotNull())
    events_day = events_day.withColumn('event_id', F.monotonically_increasing_id()) 

    geo = spark.read.csv(geo_cities, sep =';',header = True)
    geo = geo.withColumn('lat', F.regexp_replace('lat', ',', '.').cast('double')) \
                    .withColumn('lng', F.regexp_replace('lng', ',', '.').cast('double')) \
                    .select( F.col('city') ,F.col('lat').alias('lat_2'), F.col('lng').alias('lon_2'))

    new = events_day.crossJoin(geo)

    new = new.withColumn( 'km' , 2 * 6371 * F.asin(F.sqrt(F.sin(((F.radians(F.col("lat_2"))) - (F.radians(F.col("lat")))) / 2)**2  
                                                    + F.cos((F.radians(F.col("lat"))))*F.cos((F.radians(F.col("lat_2"))))
                                                    *F.sin(((F.radians(F.col("lon_2"))) - (F.radians(F.col("lon"))))/2)**2)))

    window = Window().partitionBy("event_id")
    min_km = new.withColumn("min_km", F.min('km').over(window)).filter(F.col('km') == F.col('min_km')).persist()

    df_city = min_km.withColumn('month',F.trunc(F.col('date'), 'month'))\
                    .withColumn('week',F.trunc(F.col('date'), 'week'))\
                    .drop('lat', 'lon', 'event_id', 'lat_2', 'lon_2', 'km', 'min_km')

    window_week = Window().partitionBy('week', 'city')
    window_month = Window().partitionBy('month', 'city')
    window = Window().partitionBy('event.message_from', 'city').orderBy(F.col('date'))

    df_city_rn = df_city.withColumn('rn',F.row_number().over(window))\
                        .withColumn('week_message',F.sum(F.when(df_city.event_type == 'message',1).otherwise(0)).over(window_week))\
                        .withColumn('week_reaction',F.sum(F.when(df_city.event_type == 'reaction',1).otherwise(0)).over(window_week))\
                        .withColumn('week_subscription',F.sum(F.when(df_city.event_type == 'subscription',1).otherwise(0)).over(window_week))\
                        .withColumn('week_user',F.sum(F.when(F.col('rn') == 1,1).otherwise(0)).over(window_week))\
                        .withColumn('month_message',F.sum(F.when(df_city.event_type == 'message',1).otherwise(0)).over(window_month))\
                        .withColumn('month_reaction',F.sum(F.when(df_city.event_type == 'reaction',1).otherwise(0)).over(window_month))\
                        .withColumn('month_subscription',F.sum(F.when(df_city.event_type == 'subscription',1).otherwise(0)).over(window_month))\
                        .withColumn('month_user',F.sum(F.when(F.col('rn') == 1,1).otherwise(0)).over(window_month)).persist()
                            
    df_zones = df_city_rn.select('month', 'week', F.col('city').alias('zone_id'), 'week_message',
                            'week_reaction', 'week_subscription', 'week_user', 'month_message',
                            'month_reaction', 'month_subscription', 'month_user').distinct()               


    return df_zones

def main():
    geo_events_source = sys.argv[1]
    geo_cities = sys.argv[2]
    destination_path = sys.argv[3]

    spark = (SparkSession.builder
                        .master('yarn')
                        .appName('sliced_by_zones')
                        .getOrCreate())
    


    df_zones_slicing = zones_slicing(geo_events_source , geo_cities, spark)

    df_zones_slicing.write.parquet(destination_path + f'zones_slicing')

if __name__ == "__main__":
        main()
