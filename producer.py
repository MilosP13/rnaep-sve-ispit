import json  
import time
import requests  
from kafka import KafkaProducer  


producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'));  


api_key = '1b4bb4dda2969a28dd8c28abb2cceb6b'

city = 'Smederevo'  

lat=44.66278

lon=20.93

polution_api_url = f'http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={api_key}'  




def fetch():
    try:
        response = requests.get(polution_api_url)
        
        if response.status_code == 200:
            polution_data = response.json()
            return polution_data
        else:
            print(f"Could fetch data from API: {response.status_code}")
            return None
    except Exception as e:
        print(f"Exception occured:{str(e)}")
        return None

def send_data():
    while True:
        polution_data = fetch()
        if polution_data:
            producer.send('raw-data',value=polution_data)
            print("Sent data to Kafka:", polution_data)

        time.sleep(10)

if __name__ == "__main__":
    send_data()