from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import requests
from kafka import KafkaProducer
import time
API_key = 'TOP_sercet_api' # your api key
bangkok_districts = {
    "Phra Nakhon": {"latitude": 13.7563, "longitude": 100.5018},
    "Dusit": {"latitude": 13.7842, "longitude": 100.5117},
    "Nong Chok": {"latitude": 13.8583, "longitude": 100.8659},
    "Bang Rak": {"latitude": 13.7259, "longitude": 100.5283},
    "Bang Khen": {"latitude": 13.8644, "longitude": 100.5843},
    "Bang Kapi": {"latitude": 13.7554, "longitude": 100.6309},
    "Pathum Wan": {"latitude": 13.7445, "longitude": 100.5342},
    "Pom Prap Sattru Phai": {"latitude": 13.7504, "longitude": 100.5097},
    "Phra Khanong": {"latitude": 13.7088, "longitude": 100.6022},
    "Min Buri": {"latitude": 13.8053, "longitude": 100.7550},
    "Lat Krabang": {"latitude": 13.7262, "longitude": 100.7562},
    "Yannawa": {"latitude": 13.6896, "longitude": 100.5369},
    "Samphanthawong": {"latitude": 13.7408, "longitude": 100.5137},
    "Phaya Thai": {"latitude": 13.7719, "longitude": 100.5385},
    "Thon Buri": {"latitude": 13.7229, "longitude": 100.4907},
    "Bangkok Yai": {"latitude": 13.7314, "longitude": 100.4806},
    "Huai Khwang": {"latitude": 13.7788, "longitude": 100.5759},
    "Khlong San": {"latitude": 13.7348, "longitude": 100.5042},
    "Taling Chan": {"latitude": 13.7702, "longitude": 100.4429},
    "Bangkok Noi": {"latitude": 13.7634, "longitude": 100.4735},
    "Bang Khun Thian": {"latitude": 13.6703, "longitude": 100.4578},
    "Phasi Charoen": {"latitude": 13.7214, "longitude": 100.4357},
    "Nong Khaem": {"latitude": 13.7153, "longitude": 100.3792},
    "Rat Burana": {"latitude": 13.6865, "longitude": 100.4925},
    "Bang Phlat": {"latitude": 13.7951, "longitude": 100.5015},
    "Din Daeng": {"latitude": 13.7663, "longitude": 100.5562},
    "Bueng Kum": {"latitude": 13.7895, "longitude": 100.6558},
    "Sathon": {"latitude": 13.7211, "longitude": 100.5289},
    "Bang Sue": {"latitude": 13.8009, "longitude": 100.5297},
    "Chatuchak": {"latitude": 13.8166, "longitude": 100.5557},
    "Prawet": {"latitude": 13.7196, "longitude": 100.6698},
    "Khlong Toei": {"latitude": 13.7126, "longitude": 100.5582},
    "Suan Luang": {"latitude": 13.7212, "longitude": 100.6406},
    "Chom Thong": {"latitude": 13.6853, "longitude": 100.4674},
    "Don Mueang": {"latitude": 13.9125, "longitude": 100.5989},
    "Ratchathewi": {"latitude": 13.7547, "longitude": 100.5342},
    "Lat Phrao": {"latitude": 13.8078, "longitude": 100.5701},
    "Watthana": {"latitude": 13.7369, "longitude": 100.5847},
    "Sai Mai": {"latitude": 13.9248, "longitude": 100.6385},
    "Khan Na Yao": {"latitude": 13.8159, "longitude": 100.6669},
    "Saphan Sung": {"latitude": 13.7610, "longitude": 100.6726},
    "Wang Thonglang": {"latitude": 13.7889, "longitude": 100.6120},
    "Khlong Sam Wa": {"latitude": 13.8647, "longitude": 100.7351},
}
default_arg = {
    'owner' : 'copter',
    'start_date': datetime(2025,2,19, 12, 00)
}
def get_data(lat, lon):
    url = f'http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={API_key}'
    res = requests.get(url)
    res = res.json()
    return res

def get_data_weather(lat, lon):
    url2 = f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_key}'
    res2 = requests.get(url2)
    res2 = res2.json()
    return res2

api_url_sensor = "https://api.airgradient.com/public/api/v1/locations/measures/current?token=ba29ff46-468c-4a4e-a1bc-7f308aace0b2" 

def call_api(api_url_sensor):
    try:
        response = requests.get(api_url_sensor)
        response.raise_for_status() # raise an exception for bad status codes.
        data = response.json()
        beautified_json = json.dumps(data, indent=4)
        print(beautified_json)

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        
    return data
        
        
def format_data_sensor(data):
    import time
    data = data[0]
    formatted_data = {}
    formatted_data['locationName'] = data['locationName']
    formatted_data['timestamp'] = time.time() 
    formatted_data['pm02'] = data['pm02']
    formatted_data['atmp'] = data['atmp']
    formatted_data['rhum'] = data['rhum']
    formatted_data['rco2'] = data['rco2']   
    
    return formatted_data
    
def format_data(res,name): # format the data to be sent to kafka
    data = {}
    data['district'] = name
    data['timestamp'] = res['list'][0]['dt']
    data['lon'] = res['coord']['lon']
    data['lat'] = res['coord']['lat']
    data['AQI'] = res['list'][0]['main']['aqi']
    data['CO'] = res['list'][0]['components']['co']
    data['NO'] = res['list'][0]['components']['no']
    data['NO2'] = res['list'][0]['components']['no2']
    data['O3'] = res['list'][0]['components']['o3']
    data['SO2'] = res['list'][0]['components']['so2']
    data['PM2_5'] = res['list'][0]['components']['pm2_5']
    data['PM10'] = res['list'][0]['components']['pm10']
    data['NH3'] = res['list'][0]['components']['nh3']
    return data

def format_data_weather(res2,name): # format the data to be sent to kafka
    data2 = {}
    data2['district'] = name
    data2['timestamp'] = res2['dt']
    data2['temp'] = res2['main']['temp']
    data2['feels_like'] = res2['main']['feels_like']
    data2['pressure'] = res2['main']['pressure']
    data2['humidity'] = res2['main']['humidity']
    data2['clouds'] = res2['clouds']['all']
    data2['wind_speed'] = res2['wind']['speed']
    data2['wind_deg'] = res2['wind']['deg']
    data2['weather_main'] = res2['weather'][0]['main']
    data2['weather_desc'] = res2['weather'][0]['description']
    return data2
def steam_data():
    import logging
    from kafka import KafkaConsumer
    import json
    import pandas as pd
    import time

    # Kafka Producers
    producer = KafkaProducer(bootstrap_servers=['broker1:29092'], max_block_ms=5000)
    producer2 = KafkaProducer(bootstrap_servers=['broker2:29093'], max_block_ms=5000)
    producer3 = KafkaProducer(bootstrap_servers=['broker3:29094'], max_block_ms=5000)
    curr_time = time.time()
    while True:
        if time.time() - curr_time > 86400:  # 86400 seconds in 24 hours
            break
        try:
            # Fetch and send sensor data
            res = call_api(api_url_sensor)
            data = format_data_sensor(res)
            producer3.send('airpollution_from_sensor', json.dumps(data).encode('utf-8'))
            logging.info('Sensor data sent to Kafka')
        except Exception as e:
            logging.error(f"Error in sending sensor data to Kafka: {e}")

        for district, coords in bangkok_districts.items():
            try:
                # Fetch and send air pollution data
                res = get_data(coords['latitude'], coords['longitude'])
                data = format_data(res, district)
                producer.send('airpollution_from_API', json.dumps(data).encode('utf-8'))

                # Fetch and send weather data
                res2 = get_data_weather(coords['latitude'], coords['longitude'])
                data2 = format_data_weather(res2, district)
                producer2.send('weather_from_API', json.dumps(data2).encode('utf-8'))

                logging.info(f"Data for {district} sent to Kafka")
            except Exception as e:
                logging.error(f"Error in sending data for {district} to Kafka: {e}")


        time.sleep(30)  # Sleep for 20 seconds
                

with DAG('airpollution',
         default_args=default_arg,
         schedule_interval='@daily', # run the DAG daily
         catchup=False) as dag:
    
    steaming_task = PythonOperator(
        task_id='streaming_data',
        python_callable=steam_data)