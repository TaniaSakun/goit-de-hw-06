from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, avg, struct, lit, current_timestamp, window, to_json
import time
from utils import create_kafka_producer, create_kafka_consumer, generate_sensor_data, get_spark_schema, generate_uuid
from configs import kafka_config
import os


def produce_data():
    producer = create_kafka_producer(kafka_config)
    my_name = "tania"
    topic_name = f"{my_name}_building_sensors"

    for i in range(20):
        try:
            data = generate_sensor_data()
            producer.send(topic_name, key=generate_uuid(), value=data)  # Call generate_uuid()
            producer.flush()
            print(f"Message {i} sent to topic '{topic_name}' successfully.")
            time.sleep(2)
        except Exception as e:
            print(f"An error occurred: {e}")

    producer.close()


def stream_data():
    os.environ['PYSPARK_SUBMIT_ARGS'] = (
        '--packages '
        'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,' 
        'org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.4 '
        'pyspark-shell')

    spark = (SparkSession.builder
             .appName("KafkaStreaming")
             .master("local[*]")
             .config("spark.executor.memory", "4g")
             .config("spark.driver.host", "127.0.0.1")
             .config("spark.driver.bindAddress", "127.0.0.1")
             .getOrCreate())


    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config", 
                'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
        .option("subscribe", "yk_building_sensors") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", "5") \
        .load()

    json_schema = get_spark_schema()


    clean_df = (df.selectExpr("CAST(key AS STRING) AS key_deserialized", "CAST(value AS STRING) AS value_deserialized", "*")
                .drop('key', 'value')
                .withColumnRenamed("key_deserialized", "key")
                .withColumn("value_json", from_json(col("value_deserialized"), json_schema))
                .withColumn("sensor_id", col("value_json.sensor_id"))
                .withColumn("time", to_timestamp(col("value_json.time"), "yyyy-MM-dd HH:mm:ss"))
                .withColumn("temperature", col("value_json.temperature"))
                .withColumn("humidity", col("value_json.humidity"))
                .drop("value_json", "value_deserialized"))

  
    agg_df = (clean_df
              .withWatermark("time", "10 seconds")
              .groupBy(window(col("time"), "1 minute", "30 seconds"), col("sensor_id"))
              .agg(avg("temperature").alias("t_avg"), avg("humidity").alias("h_avg")))


    alerts_df = spark.read.option("header", "true").csv("alerts_conditions.csv")
    alerts_df = alerts_df.withColumn("humidity_min", col("humidity_min").cast("int")) \
                         .withColumn("humidity_max", col("humidity_max").cast("int")) \
                         .withColumn("temperature_min", col("temperature_min").cast("int")) \
                         .withColumn("temperature_max", col("temperature_max").cast("int"))

    joined_df = agg_df.crossJoin(alerts_df).filter(
        col("t_avg").between(col("temperature_min"), col("temperature_max")) |
        col("h_avg").between(col("humidity_min"), col("humidity_max"))
    )

 
    prepared_df = joined_df.withColumn(
        "window_start", col("window.start").cast("timestamp")
    ).withColumn(
        "window_end", col("window.end").cast("timestamp")
    ).withColumn(
        "message", 
        lit("It's too hot") 
    ).withColumn(
        "timestamp", current_timestamp()
    ).select(
        to_json(struct(
            struct(col("window_start").alias("start"), col("window_end").alias("end")).alias("window"),
            col("t_avg"),
            col("h_avg"),
            col("message"),
            col("timestamp")
        )).alias("value")
    )


    prepared_df.writeStream \
        .trigger(processingTime='10 seconds') \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
        .option("topic", "yk_streaming_out") \
        .option("kafka.security.protocol", "SASL_PLAINTEXT") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='VawEzo1ikLtrA8Ug8THa';") \
        .option("checkpointLocation", "/tmp/checkpoints-3") \
        .start() \
        .awaitTermination()
        
    spark.stop()
        

def consume_data():
    consumer = create_kafka_consumer(kafka_config)
    my_name = "tania"
    topic_name = f"{my_name}_building_sensors" 

    consumer.subscribe([topic_name])
    print(f"Subscribed to '{topic_name}'")

    try:
        time.sleep(1)  
        for message in consumer:
            print(f"Received message: {message.value}")
    except Exception as e:
        print(f"An error occurred: {e}")

    consumer.close()
