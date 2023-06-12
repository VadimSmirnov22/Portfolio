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


def df_friends(geo_events_source: str, geo_cities: str, spark: SparkSession) -> DataFrame:

    #подготовка датасетов
    events = spark.read.parquet(geo_events_source)

    events_day = events.filter(F.col("lat").isNotNull() & F.col("lat").isNotNull() & 
                            (events.event_type == "message"))

    geo = spark.read.csv(geo_cities, sep =';',header = True)

    geo = geo.withColumn('lat', F.regexp_replace('lat', ',', '.').cast('double')) \
                            .withColumn('lng', F.regexp_replace('lng', ',', '.').cast('double')) \
                            .select( F.col('city') ,F.col('lat').alias('lat_2'), F.col('lng').alias('lon_2'))

    new = events_day.crossJoin(geo)

    new = new.withColumn( 'km' , 2 * 6371 * F.asin(F.sqrt(F.sin(((F.radians(F.col("lat_2"))) - (F.radians(F.col("lat")))) / 2)**2  
                                                    + F.cos((F.radians(F.col("lat"))))*F.cos((F.radians(F.col("lat_2"))))
                                                    *F.sin(((F.radians(F.col("lon_2"))) - (F.radians(F.col("lon"))))/2)**2)))

    window = Window().partitionBy('event.message_from', "event.message_id")
    min_km = new.withColumn("min_km", F.min('km').over(window)).filter(F.col('km') == F.col('min_km'))

    users = min_km.select('event', 'event.message_from', 'event.message_to', 'date', 'city', 'lat', 'lon')\
                        .filter(F.col('message_to').isNotNull()).dropDuplicates()

    channels = events.filter((events.event_type == "subscription")).select(F.col("event.subscription_channel")\
        .alias("subscription_channel") ,F.col("event.user").alias("subscription_user")).distinct()              

    combine = users.join(channels, users.message_from == channels.subscription_user, 'inner').persist()

    #крайний город и правка городов
    window = Window().partitionBy('message_from').orderBy('date')
    city_row_num = combine.withColumn('row',F.row_number().over(window))

    window = Window().partitionBy('message_from')
    city_max = city_row_num.withColumn('max',F.max('row').over(window)).persist()

    users_city_clean = city_max.filter(F.col('row') == F.col('max')).withColumn('zone_id', (F.when((F.col('city') != 'Gold Coast') & (F.col('city') != 'Cranbourne')  
                        & (F.col('city') != 'Newcastle')
                        & (F.col('city') != 'Wollongong') & (F.col('city') != 'Geelong') & (F.col('city') != 'Townsville')
                        & (F.col('city') != 'Ipswich') & (F.col('city') != 'Cairns') & (F.col('city') != 'Toowoomba')
                        & (F.col('city') != 'Ballarat') & (F.col('city') != 'Bendigo') & (F.col('city') != 'Launceston')
                        & (F.col('city') != 'Mackay') & (F.col('city') != 'Rockhampton') & (F.col('city') != 'Maitland')
                        & (F.col('city') != 'Bunbury'), F.col('city')).otherwise('Brisbane'))) \
                        .withColumn('TIME', to_timestamp(F.col('date'))) \
                        .withColumn('timezone', F.concat(F.lit('Australia'), F.lit('/'),  F.col('zone_id'))) \
                        .withColumn('local_time', F.from_utc_timestamp(F.col('TIME'), F.col('timezone'))) \
                        .drop('row', 'max', 'city', 'TIME', 'date' , 'timezone', 'subscription_user', 'event')   

        # добавил столбец сообщества, чтобы далее найти только тех, кто состоит в одном
    users_city_clean_2 = users_city_clean.select(
      F.col('message_from').alias('message_from_2'),
      F.col('message_to').alias('message_to_2'),
      F.col('subscription_channel').alias('subscription_channel_2'))

        # все потенциальные пары, состоящие в одном сообществе
    all_pairs = users_city_clean.crossJoin(users_city_clean_2) \
      .filter(F.col('message_from') != F.col('message_from_2')) \
      .filter(F.col('subscription_channel') == F.col('subscription_channel_2'))
    
        # все пары, которые переписывались, и их перевёрнутые версии (имитируем взаимные переписки)
    all_communications = users_city_clean.select('message_from', 'message_to') \
      .union(users_city_clean.select('message_to', 'message_from')) \
      .distinct()
    
        # оставляем только те пары, которые не переписывались
    strangers = all_pairs.join(all_communications,
        (all_pairs.message_from == all_communications.message_from) &
        (all_pairs.message_to == all_communications.message_from), "leftanti")\
       .select(all_pairs.message_from, all_pairs.message_to, all_pairs.lat
        , all_pairs.lon, all_pairs.zone_id, all_pairs.local_time).distinct().persist()
    
    
    #второй пользователь
    users_2_d = strangers.alias('users_2')

    users_2 = users_2_d.select(F.col('message_from').alias('message_from_2'),
            F.col('lat').alias('lat_2'),
            F.col('lon').alias('lon_2'))

    #соединяем
    crossJoin_users = strangers.crossJoin(users_2)
    
    crossJoin_users_clean = crossJoin_users.filter((F.col('message_from') != F.col('message_from_2')))

    #поиск километра
    users_km = crossJoin_users_clean.withColumn( 'km' , 2 * 6371 * F.asin(F.sqrt(F.sin(((F.radians(F.col("lat_2"))) - (F.radians(F.col("lat")))) / 2)**2  
                                                    + F.cos((F.radians(F.col("lat"))))*F.cos((F.radians(F.col("lat_2"))))
                                                    *F.sin(((F.radians(F.col("lon_2"))) - (F.radians(F.col("lon"))))/2)**2)))

    #минимальный километр и финальные столбцы
    df_users = users_km.filter(F.col('km') < 1 ).withColumn('processed_dttm', F.current_timestamp()) \
                                    .select(F.col('message_from').alias('user_left'),
                                        F.col('message_from_2').alias('user_right'),
                                        'processed_dttm', 'zone_id', 'local_time').dropDuplicates()      
    return df_users

def main():
    geo_events_source = sys.argv[1]
    geo_cities = sys.argv[2]
    destination_path = sys.argv[3]

    spark = (SparkSession.builder
                        .master('yarn')
                        .appName('sliced_by_zones')
                        .getOrCreate())
    

    df_friends_d = df_friends(geo_events_source,  geo_cities, spark)

    df_friends_d.write.parquet(destination_path + f'df_friends')

if __name__ == "__main__":
        main()
