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
| {2025-02-01 21:57...}|1|36.808||49.38|
| {2025-02-01 21:57...}|1|36.808||49.38|

Batch: 102

|window|  sensor_id|avg_temperature| avg_humidity| 
| ------- | ------- | ------- | ------- |
| {2025-02-01 22:01...}|2| 35.87|79.21|
| {2025-02-01 22:01...}|2| 35.87|79.21|


**Task 5.**
Subscribed to 'tania_building_sensors'
{'window':{'start':'2025-02-02T22:30:00.000+00:00','end':'2025-02-02T22:31:00.000+00:00'},'t_avg':42.95,'h_avg':62.6,'code':'104','message':'It\'s too hot','timestamp':'2025-02-02 22:30:51.907777'}
{'window':{'start':'2025-02-02T22:30:00.000+00:00','end':'2025-02-02T22:31:00.000+00:00'},'t_avg':36.28,'h_avg':48.4,'code':'104','message':'It\'s too hot','timestamp':'2025-02-02 22:30:59.907777'}
{'window':{'start':'2025-02-02T22:30:00.000+00:00','end':'2025-02-02T22:31:00.000+00:00'},'t_avg':38.4,'h_avg':74.55,'code':'104','message':'It\'s too hot','timestamp':'2025-02-02 22:31:05.907777'}
{'window':{'start':'2025-02-02T22:30:00.000+00:00','end':'2025-02-02T22:31:00.000+00:00'},'t_avg':38.28,'h_avg':31.01,'code':'104','message':'It\'s too hot','timestamp':'2025-02-02 22:31:11.907777'}
{'window':{'start':'2025-02-02T22:30:00.000+00:00','end':'2025-02-02T22:31:00.000+00:00'},'t_avg':35.17,'h_avg':24.0,'code':'103','message':'It\'s too cold','timestamp':'2025-02-02 22:30:57.907777'}