import findspark
import sys 
import os
import datetime
findspark.init()
findspark.find()
from pyspark.sql.functions import to_timestamp

import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from datetime import datetime

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ["JAVA_HOME"] = "/usr"
os.environ["SPARK_HOME"] = "/usr/lib/spark"
os.environ["PYTHONPATH"] = "/usr/local/lib/python3.8"


def event_with_city(geo_events_source: str, geo_cities: str, spark: SparkSession) -> DataFrame:

    events = spark.read.parquet(geo_events_source)

    events_day = events.filter(F.col("lat").isNotNull() & F.col("lon").isNotNull() &
                            (events.event_type == "message"))

    events_day =  events_day.select(F.col('event.message_from'), 
                                F.col('event.datetime'),
                                F.col('event.message_id'), 
                                F.col('date'), 
                                F.col('lat'),  
                                F.col('lon'))


    geo = spark.read.csv(geo_cities, sep =';', header = True)

    geo = geo.withColumn('lat', F.regexp_replace('lat', ',', '.').cast('double')) \
                        .withColumn('lng', F.regexp_replace('lng', ',', '.').cast('double')) \
                        .select( F.col('city') ,F.col('lat').alias('lat_2'), F.col('lng').alias('lon_2'))

    new = events_day.crossJoin(geo)

    new = new.withColumn( 'km' , 2 * 6371 * F.asin(F.sqrt(F.sin(((F.radians(F.col("lat_2"))) - (F.radians(F.col("lat")))) / 2)**2  
                                                        + F.cos((F.radians(F.col("lat"))))*F.cos((F.radians(F.col("lat_2"))))
                                                        *F.sin(((F.radians(F.col("lon_2"))) - (F.radians(F.col("lon"))))/2)**2)))

    window = Window().partitionBy('message_from', "message_id")

    new_2 = new.withColumn("min_km", F.min('km').over(window)) \
                    .select('message_from', 'datetime', 'message_id', 'date', 'city', 'lat', 'lon', 'km', 'min_km').persist()

    new_3 = new_2.filter( new_2.km == new_2.min_km).drop('min_km')

    new_4 =  new_3.select('message_from', 'city', 'date').distinct().persist()

    window = Window().partitionBy('message_from', "city").orderBy('date')
    new_5 = new_4.withColumn('rn', F.row_number().over(window))

    new_6 =  new_5.selectExpr('*' ,'date_sub(date, rn) as diff').persist()

    #кол-во городов
    city_count_df = new_6.drop('date', 'rn').distinct().orderBy('message_from', 'diff').persist()
    window = Window().partitionBy('message_from').orderBy('diff')
    city_count_lag = city_count_df.withColumn('lag', F.lag('city').over(window))
    city_count_lag_filter = city_count_lag.filter((city_count_lag.lag.isNull()) | (city_count_lag.lag != city_count_lag.city))
    window = Window().partitionBy('message_from')
    city_count = city_count_lag_filter.withColumn('travel_count',F.count('city').over(window))
    df_city_count = city_count.drop('lag','diff','city').select(F.col('message_from') \
                                                .alias('message_from_cntcity'), 'travel_count').distinct()

    #город посещения
    df = new_3.select('message_from', 'city', 'date', 'datetime')
    window = Window().partitionBy('message_from').orderBy('date')
    event_city = df.withColumn('rn_2', F.row_number().over(window))

    window = Window().partitionBy('message_from')
    event_city_2 = event_city.withColumn('max_rn_2', F.max('rn_2').over(window))

    df_event_city = event_city_2.withColumn('city_event', F.when(event_city_2.max_rn_2 == event_city_2.rn_2, event_city_2.city). \
        otherwise("NOT")).filter(F.col('city_event') != "NOT")\
        .select(F.col('message_from').alias('message_from_event'), 'city_event', 'date', 'datetime')

    #список городов
    df_travel_array = (city_count.withColumn('lst', F.col('city').alias('lst'))
                            .groupBy('message_from')
                            .agg( F.concat_ws(',', F.collect_list('lst').alias('b_list')).alias('travel_array'))) \
                            .select(F.col('message_from').alias('message_from_trar'), 'travel_array')

    #домашний город
    window = Window().partitionBy('message_from', "city", 'diff') 
    new_7 = new_6.withColumn('days_count', F.count('diff').over(window)) 
    new_8 = new_7.withColumn("home_city", F.when(new_7.days_count > 26 , new_7.city)).filter(F.col('days_count') >26).persist()
    df_home_city = new_8.withColumn("home", F.last('home_city', True) \
                    .over(Window.partitionBy('message_from').orderBy('diff').rowsBetween(-sys.maxsize, 0))) \
                    .select(F.col('message_from').alias('message_from_hc'), F.col('home').alias('home_city')).distinct()

    df_join = df_city_count.join(df_event_city, df_city_count.message_from_cntcity == df_event_city.message_from_event, how="left") \
            .join(df_travel_array, df_city_count.message_from_cntcity == df_travel_array.message_from_trar, how="left") \
            .join(df_home_city, df_city_count.message_from_cntcity == df_home_city.message_from_hc, how="left") \
            .drop('message_from_trar', 'message_from_event', 'message_from_hc').persist()

    df_with_time = df_join.withColumn('city_true', (F.when((F.col('city_event') != 'Gold Coast') & (F.col('city_event') != 'Cranbourne')   
                            & (F.col('city_event') != 'Newcastle') 
                            & (F.col('city_event') != 'Wollongong') & (F.col('city_event') != 'Geelong') & (F.col('city_event') != 'Townsville') 
                            & (F.col('city_event') != 'Ipswich') & (F.col('city_event') != 'Cairns') & (F.col('city_event') != 'Toowoomba') 
                            & (F.col('city_event') != 'Ballarat') & (F.col('city_event') != 'Bendigo') & (F.col('city_event') != 'Launceston') 
                            & (F.col('city_event') != 'Mackay') & (F.col('city_event') != 'Rockhampton') & (F.col('city_event') != 'Maitland') 
                            & (F.col('city_event') != 'Bunbury'), F.col('city_event')).otherwise('Brisbane')))\
                                .withColumn('TIME', to_timestamp(F.col('datetime')))\
                                .withColumn('timezone', F.concat(F.lit('Australia'), F.lit('/'),  F.col('city_true')))\
                                .withColumn('local_time', F.from_utc_timestamp(F.col('TIME'), F.col('timezone')))\
                                .select(F.col('message_from_cntcity').alias('user_id'), F.col('city_event').alias('act_city'), 
                                              'home_city', 'travel_count', 'travel_array', 'local_time')


    return df_with_time






def main():
    geo_events_source = sys.argv[1]
    geo_cities = sys.argv[2]
    destination_path = sys.argv[3]

    spark = (SparkSession.builder
                        .master('yarn')
                        .appName('sliced_by_user')
                        .getOrCreate())
    
    df_city = event_with_city(geo_events_source, geo_cities, spark)
    df_city.write.parquet(destination_path + f'df_city')



if __name__ == "__main__":
        main()    
