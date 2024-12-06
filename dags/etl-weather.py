from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import requests
import json

# Latitude and longitude for the desired location (Pune in this case)
LATITUE = '18.5204'
LONGITUDE = '73.8567'
POSTGRES_CONNECTION_ID = 'postgres_default'
API_CONNECTION_ID = 'open_meteo_api'

default_args = {
    'owner' : 'airflow',
    'start_date': days_ago(1)
    
}

# DAG
with DAG (dag_id = 'weather_etl_pipeline',
          default_args = default_args,
          schedule_interval = '@daily',
          catchup = False) as dags:
    
    @task()
    def extract_weather_data():
        """Extracting weather data from Open-Meteo API with Airflow Connection"""
        
        # Use HTTP Hook to get connection details from Airflow Connection
        
        http_hook = HttpHook(http_conn_id=API_CONNECTION_ID, method='GET')
        
        # Building API endpoint
        # $ curl "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&current=temperature_2m,wind_speed_10m&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"
        endpoint = f'/v1/forecast?latitude={LATITUE}&longitude={LONGITUDE}&current_weather=true'
        
        # Make the request via the HTTP Hook
        
        response = http_hook.run(endpoint)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather Data: {response.status_code}")
        
    @task()
    def transform_weather_data(weather_data):
        '''Transform the extracted weather data'''
        
        current_weather = weather_data['current_weather']
        transformed_data = {
            'latitude': LATITUE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode']
        }
        return transformed_data
    
    @task()
    def load_weather_data(transformed_data):
        '''Load the transformed data into postgreSQL'''
        
        pg_hook = PostgresHook( POSTGRES_CONNECTION_ID = POSTGRES_CONNECTION_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        #create table if it dosent exist
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS weather_data (
                latitude FLOAT,
                longitude FLOAT,
                temperature FLOAT,
                windspeed FLOAT,
                winddirection FLOAT,
                weathercode INT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """
        )
        
        # Insert Transformed data into the table
        
        cursor.execute(
            """
            INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
            VALUES (%s,%s,%s,%s,%s,%s)
            """, (
                transformed_data['latitude'],
                transformed_data['longitude'],
                transformed_data['temperature'],
                transformed_data['windspeed'],
                transformed_data['winddirection'],
                transformed_data['weathercode']
            )
        )
        
        conn.commit()
        conn.close()
        
        
    # DAG WORFLOW - ETL PIPELINE
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)