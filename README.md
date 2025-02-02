# goit-de-hw-06
The repository for the 6th GoItNeo Data Engineering homework

## Task Description:

1. Data Stream Generation:
The input data is from a Kafka topic, the same as in the previous assignment. Generate a data stream that includes id, temperature, humidity, and timestamp. You can use the script and topic you previously created.

2. Data Aggregation:
Read the data stream that you generated in the first part. Using a Sliding Window of 1-minute length, a sliding interval of 30 seconds, and a watermark duration of 10 seconds, calculate the average temperature and humidity.

3. Familiarizing with Alert Parameters:
Your manager likes to change alert criteria often. Therefore, instead of deploying the code each time, the alert parameters are specified in a file:

alerts_conditions.csv

This file contains the maximum and minimum values for temperature and humidity, along with the alert message and code. The values -999,-999 indicate that these parameters are not used for this particular alert.

Look at the data in the file. It should be intuitive. You need to read the data from the file and use it to configure the alerts.

4. Building Alert Definitions:
After calculating the average values, you need to check whether they meet the criteria from the file (hint: perform a cross join and filtering).

5. Writing Data to Kafka Topic:
The resulting alerts should be written to the output Kafka topic.

Here’s an example Kafka message that is the result of this code:

```
{
  "window": {
    "start": "2024-08-18T16:08:00.000+03:00",
    "end": "2024-08-18T16:09:00.000+03:00"
  },
  "t_avg": 56.61538461538461,
  "h_avg": 58.07692307692308,
  "code": "104",
  "message": "It's too hot",
  "timestamp": "2024-08-18 16:05:50.907777"
}
```

## Task Results:
**Task 1.** Message 0-19 sent to topic 'tania_building_sensors' successfully.

**Task 2.** 
Batch: 100

|window|  sensor_id|avg_temperature| avg_humidity| 
| ------- | ------- | ------- | ------- |
| {2025-02-01 21:53...}|1|35.248333333333335|49.464999999999996|
| {2025-02-01 21:53...}|1|35.980000000000004|42.876|
| {2025-02-01 21:54...}|1|37.0775|32.9925|

Batch: 101

|window|  sensor_id|avg_temperature| avg_humidity| 
| ------- | ------- | ------- | ------- |
| {2025-02-01 21:57...}|1|36.808|49.38|
| {2025-02-01 21:57...}|1|36.808|49.38|

Batch: 102

|window|  sensor_id|avg_temperature| avg_humidity| 
| ------- | ------- | ------- | ------- |
| {2025-02-01 22:01...}|2| 35.87|79.21|
| {2025-02-01 22:01...}|2| 35.87|79.21|


**Task 5.**

Subscribed to 'tania_building_sensors'

Received message: {'window': {'start': '2025-01-10T23:36:30.000+02:00' 36:30.000+02:00', 'end': ' 'avg_temperature': 'avg_temperature': 'avg_humidity': 'avg_humidity': 'code': 'code': 'message': 'message': "It's too wet", 'timestamp': '2025:40:55.537+02:00'} 

Received message: {'window': {'start': 2025-02-02T22:30:00.000+00:00', 'end': '2025-01-10T23:41:00.000+02:00'}, 'avg_temperature': 33.04333333333333, 'avg_humidity': 37.973333333333336, 'code': '101', "message': "It's too dry", 'timestamp’:’2025:40:55.537+02:00’} 

Received message: {'window': {'start': '22025-02-02T22:30:00.000+00:00', 'end': '2025-01-10T23:41:00.000+02:00'}, 'avg_temperature': 37.848, 'avg_humidity': 35.326, 'code': '101', 'message': "It's too dry", 'timestamp': '2025:40:55.537+02:00’ 

Received message: {'window': {'start': '2025-02-02T22:30:00.000+00:00', 'end': '2025-01-10T23:43:00.000+02:00'}, 'avg-temperature': 35.07055555555556, 'avg_humidity': 39.80722222222223, 'code': '101', 'message': "It's too dry", 'timestamp’:’2025:40:55.537+02:00’} 

Received message: {'window': {'start': '2025-02-02T22:30:00.000+00:00', 'end': '2025-01-10T23:43:30.000+02:00'}, 'avg_temperature': 32.55, 'avg_humidity': 38.3375, 'code': '101', 'message': "It's too dry", 'timestamp': '2025:40:55.537+02:00' 
