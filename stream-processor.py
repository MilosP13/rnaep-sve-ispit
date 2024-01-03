from kafka import KafkaConsumer, KafkaProducer
import requests, json


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
            aqi = ((iaqi[i] - iaqi[i-1]) / (c[i] - c[i-1])) * (pm2_5 - c[i-1]) + iaqi[i-1] + 120
            return round(aqi)

    return 500

#konvertujem pm25 u AQI
def calculate_aqi_pm10(pm10):
    c = [0, 55, 155, 255, 355, 425, 505, 605]
    iaqi = [0, 50, 100, 150, 200, 300, 400, 500]

    for i in range(1, len(c)):
        if pm10 <= c[i]:
            aqi = ((iaqi[i] - iaqi[i-1]) / (c[i] - c[i-1])) * (pm10 - c[i-1]) + iaqi[i-1] + 70
            return round(aqi)

    return 500    


for message in consumer:
    #uzmi json fajl
    json_data = message.value
    print(json_data)


    # ovde procesiras
    
    pm25 = json_data['list'][0]['components']['pm2_5']
    aqi_pm25 = calculate_aqi_pm25(pm25)
    print(aqi_pm25)

    pm10 = json_data['list'][0]['components']['pm10']
    aqi_pm10 = calculate_aqi_pm10(pm10)
    print(aqi_pm10)
    
    if aqi_pm25 > aqi_pm10:
        processed_data = {
                'aqi': aqi_pm25,
                'glavni': "PM2.5"
        }
    
    else:
        processed_data = {
                'aqi': aqi_pm10,
                'glavni': "PM10"
        }

    

    producer.send(output_topic, value= processed_data)
    producer.flush()


#nikad
consumer.close()
producer.close()