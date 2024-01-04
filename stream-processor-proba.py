from kafka import KafkaConsumer, KafkaProducer
import requests, json
import datetime


#conf
bootstrap_servers = 'localhost:9092'
input_topic = 'raw-data'
output_topic = 'proc-data'

#consum
consumer = KafkaConsumer(input_topic,bootstrap_servers=bootstrap_servers,value_deserializer=lambda x: json.loads(x.decode('utf-8')))

#produc
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,value_serializer=lambda x: json.dumps(x).encode('utf-8'))


#konvertujem pm25 u AQI
def calculate_aqi_pm25(pm2_5):
    c = [0, 12.1, 35.5, 55.5, 150.5, 250.5, 350.5, 500.5]
    iaqi = [0, 50, 100, 150, 200, 300, 400, 500]

    for i in range(1, len(c)):
        if pm2_5 <= c[i]:
            aqi = ((iaqi[i] - iaqi[i-1]) / (c[i] - c[i-1])) * (pm2_5 - c[i-1]) + iaqi[i-1] + 60
            return round(aqi)

    return 500

#konvertujem pm25 u AQI
def calculate_aqi_pm10(pm10):
    c = [0, 55, 155, 255, 355, 425, 505, 605]
    iaqi = [0, 50, 100, 150, 200, 300, 400, 500]

    for i in range(1, len(c)):
        if pm10 <= c[i]:
            aqi = ((iaqi[i] - iaqi[i-1]) / (c[i] - c[i-1])) * (pm10 - c[i-1]) + iaqi[i-1] + 60
            return round(aqi)

    return 500    


for message in consumer:
    #uzmi json fajl
    json_data = message.value
    


    # ovde procesiras
    

    pm2_5_values = [
    entry['components']['pm2_5']
    for entry in json_data['list']
    ]

    pm10_values = [
    entry['components']['pm10']
    for entry in json_data['list']
    ]

    if pm2_5_values:
        average_pm2_5 = sum(pm2_5_values) / len(pm2_5_values)
        
        aqi_pm25 = calculate_aqi_pm25(average_pm2_5)

        processed_data_25 = {
                'aqi': aqi_pm25,
                'glavni': "PM2.5"
        }
    else:
        print("error")

    if pm10_values:
        average_pm10 = sum(pm10_values) / len(pm10_values)
        
        aqi_pm10 = calculate_aqi_pm10(average_pm10)

        processed_data_10 = {
                'aqi': aqi_pm10,
                'glavni': "PM10"
        }
    else:
        print("error")
    
    if aqi_pm25 > aqi_pm10:
        producer.send(output_topic, value= processed_data_25)
        producer.flush()
    else:
        roducer.send(output_topic, value= processed_data_10)
        producer.flush()


#nikad
consumer.close()
producer.close()