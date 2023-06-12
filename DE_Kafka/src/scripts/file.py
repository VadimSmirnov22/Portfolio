from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType


kafka_security_options = {
    'kafka.bootstrap.servers': 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091',
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.ssl.truststore.location': '/usr/local/share/ca-certificates/Yandex/YandexCA.crt'
}

KAFKA_IN_TOPIC = 'vsmirnov22_in'
KAFKA_OUT_TOPIC = 'vsmirnov22_out'


# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(batch_df, epoch_id):

    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    batch_df.persist()

    # записываем df в PostgreSQL с полем feedback
    df_for_feedback = batch_df \
        .withColumn('feedback', F.lit(None).cast(StringType())) \
        .write.format('jdbc')\
        .option('url', 'jdbc:postgresql://localhost:5432/de')\
        .option('driver', 'org.postgresql.Driver')\
        .option('dbtable', 'public.subscribers_feedback')\
        .option('user', 'jovyan')\
        .option('password', 'jovyan')\
        .mode('append')\
        .save()

    # создаём df для отправки в Kafka. Сериализация в json.
    df_for_push = batch_df.withColumn('value', F.to_json(F.struct(
        F.col('restaurant_id'),
        F.col('adv_campaign_id'),
        F.col('adv_campaign_content'),
        F.col('adv_campaign_owner'),
        F.col('adv_campaign_owner_contact'),
        F.col('adv_campaign_datetime_start'),
        F.col('adv_campaign_datetime_end'),
        F.col('client_id'),
        F.col('datetime_created'),
        F.col('trigger_datetime_created')))).select(F.col("value"))

    # отправляем сообщения в результирующий топик Kafka без поля feedback
    df_for_push.write \
        .format("kafka") \
        .options(**kafka_security_options) \
        .option('topic', KAFKA_OUT_TOPIC) \
        .option("checkpointLocation", "/lessons/checkpoints/") \
        .option("truncate", False) \
        .save()

    # очищаем память от df
    batch_df.unpersist()


# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )


# создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
spark = SparkSession.builder \
    .appName("RestaurantSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()


# читаем из топика Kafka сообщения с акциями от ресторанов 
restaurant_read_stream_df = spark.readStream \
    .format('kafka') \
    .options(**kafka_security_options) \
    .option('subscribe', KAFKA_IN_TOPIC) \
    .load()


# определяем схему входного сообщения для json
incomming_message_schema = StructType([ \
    StructField('restaurant_id', StringType(), True), \
    StructField('adv_campaign_id', StringType(), True), \
    StructField('adv_campaign_content', StringType(), True), \
    StructField('adv_campaign_owner', StringType(), True), \
    StructField('adv_campaign_owner_contact', StringType(), True), \
    StructField('adv_campaign_datetime_start', TimestampType(), True), \
    StructField('adv_campaign_datetime_end', TimestampType(), True), \
    StructField('datetime_created', TimestampType(), True) \
])    


# определяем текущее время в UTC в миллисекундах
current_timestamp_utc = int(round(datetime.utcnow().timestamp()))


# десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
deserialized_read_stream_df = restaurant_read_stream_df.withColumn("key", restaurant_read_stream_df["key"].cast("string"))\
    .withColumn("value", restaurant_read_stream_df["value"].cast("string"))\
    .withColumn("value_str", F.from_json("value", incomming_message_schema)) 

filtered_read_stream_df = deserialized_read_stream_df.select(
    F.col("value_str.restaurant_id").alias('restaurant_id'),
    F.col("value_str.adv_campaign_id").alias('adv_campaign_id'), 
    F.col('value_str.adv_campaign_content').alias('adv_campaign_content'),
    F.col('value_str.adv_campaign_owner').alias('adv_campaign_owner'),
    F.col('value_str.adv_campaign_owner_contact').alias('adv_campaign_owner_contact'),
    F.col('value_str.adv_campaign_datetime_start').alias('adv_campaign_datetime_start'),
    F.col('value_str.adv_campaign_datetime_end').alias('adv_campaign_datetime_end'),
    F.col('value_str.datetime_created').alias('datetime_created'),
    F.col('timestamp')
    )\
    .where((F.current_timestamp()<=F.col('adv_campaign_datetime_end'))&(F.current_timestamp()>=F.col('adv_campaign_datetime_start')))\
    .withWatermark('timestamp', '60 minute')


# вычитываем всех пользователей с подпиской на рестораны
subscribers_restaurant_df = spark.read \
    .format('jdbc') \
    .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
    .option('driver', 'org.postgresql.Driver') \
    .option('dbtable', 'subscribers_restaurants') \
    .option('user', 'student') \
    .option('password', 'de-student') \
    .load()
    

# джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события.
result_df = filtered_read_stream_df.join(subscribers_restaurant_df, \
                filtered_read_stream_df.restaurant_id == subscribers_restaurant_df.restaraunt_id, 'inner')\
    .withColumn("trigger_datetime_created",F.current_timestamp())\
    .select(
        F.col('restaurant_id'),
        F.col('adv_campaign_id'),
        F.col('adv_campaign_content'),
        F.col('adv_campaign_owner'),
        F.col('adv_campaign_owner_contact'),
        F.col('adv_campaign_datetime_start'),
        F.col('adv_campaign_datetime_end'),
        F.col('client_id'),
        F.col('datetime_created'),
        F.col('trigger_datetime_created')
        ).dropDuplicates(['restaurant_id',
                    'adv_campaign_id',
                    'adv_campaign_datetime_start',
                    'adv_campaign_datetime_end',
                    'client_id'])

# запускаем стриминг
result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination(100)
