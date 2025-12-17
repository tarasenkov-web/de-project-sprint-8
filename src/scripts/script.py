import os

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import from_json, to_json, col, lit, struct, unix_timestamp, current_timestamp 
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType  

def foreach_batch_function(df, epoch_id):
    if not df.isEmpty():
        current_timestamp_utc = unix_timestamp(current_timestamp())
        df_with_feedback = df.withColumn("feedback", lit("")).withColumn("trigger_datetime_created", current_timestamp_utc)
        df_with_feedback.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/de") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "subscribers_feedback") \
            .option("user", "jovyan") \
            .option("password", "jovyan") \
            .mode("append") \
            .save()

        kafka_df = df_with_feedback.select(
            "client_id", 
            "restaurant_id", 
            "adv_campaign_id", 
            "adv_campaign_content", 
            "adv_campaign_owner", 
            "adv_campaign_owner_contact", 
            "adv_campaign_datetime_start", 
            "adv_campaign_datetime_end", 
            "datetime_created",
            "trigger_datetime_created"
        )

        kafka_df.select(to_json(struct("*")).alias("value")) \
            .write \
            .format("kafka") \
            .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
            .option('kafka.security.protocol', 'SASL_SSL') \
            .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";') \
            .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
            .option("topic", 'mirumirvalentina_out') \
            .save()
        
        df.unpersist() 


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
    .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
    .option('kafka.security.protocol', 'SASL_SSL') \
    .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";') \
    .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
    .option('subscribe', 'mirumirvalentina_in') \
    .load()

# определяем схему входного сообщения для json
incomming_message_schema = StructType([
                     StructField("restaurant_id" , StringType(), True),
                     StructField("adv_campaign_id" , StringType(), True),
                     StructField("adv_campaign_content" , StringType(), True),
                     StructField("adv_campaign_owner" , StringType(), True),
                     StructField("adv_campaign_owner_contact" , StringType(), True),
                     StructField("adv_campaign_datetime_start" , LongType(), True),
                     StructField("adv_campaign_datetime_end" , LongType(), True),
                     StructField("datetime_created" , LongType(), True)])

# определяем текущее время в UTC в миллисекундах, затем округляем до секунд
current_timestamp_utc = unix_timestamp(current_timestamp())
# десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
filtered_read_stream_df  = restaurant_read_stream_df.select(from_json(col("value").cast("string"), incomming_message_schema).alias("parsed_key_value"))
flat_df = filtered_read_stream_df.select("parsed_key_value.*").where(
    (f.col("adv_campaign_datetime_start") < current_timestamp_utc) & 
    (f.col("adv_campaign_datetime_end") > current_timestamp_utc)
)

subscribers_restaurant_df = spark.read \
                    .format('jdbc') \
                      .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
                    .option('driver', 'org.postgresql.Driver') \
                    .option('dbtable', 'subscribers_restaurants') \
                    .option('user', 'student') \
                    .option('password', 'de-student') \
                    .load()

subscribers_restaurant_df.show()

result_df = flat_df.join(subscribers_restaurant_df, flat_df.restaurant_id == subscribers_restaurant_df.restaurant_id) \
    .withColumn("datetime_created", current_timestamp_utc) \
    .select(
        subscribers_restaurant_df.client_id, 
        flat_df.restaurant_id,
        flat_df.adv_campaign_id, 
        flat_df.adv_campaign_content, 
        flat_df.adv_campaign_owner, 
        flat_df.adv_campaign_owner_contact,
        flat_df.adv_campaign_datetime_start, 
        flat_df.adv_campaign_datetime_end, 
        "datetime_created"
    )

result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination() 
