from kafka import KafkaProducer, KafkaConsumer
import json
import uuid
import random
from datetime import datetime
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

def create_kafka_producer(kafka_config):
    return KafkaProducer(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: json.dumps(k).encode('utf-8')
    )

def create_kafka_consumer(kafka_config):
    return KafkaConsumer(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password'],
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        key_deserializer=lambda v: v.decode('utf-8') if v is not None else None,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id="my_consumer_group_3"
    )

def generate_sensor_data():
    sensor_id = random.randint(1, 20)
    data = {
        "sensor_id": sensor_id,
        "time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "temperature": round(25 + random.random() * (45 - 25), 2),
        "humidity": round(15 + random.random() * (85 - 15), 2)
    }
    return data

def get_spark_schema():
    return StructType([
        StructField("sensor_id", IntegerType(), True),
        StructField("time", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
    ])

def generate_uuid():
    return str(uuid.uuid4())
