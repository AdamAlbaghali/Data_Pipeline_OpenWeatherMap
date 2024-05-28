from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd
import logging

def kelvin_to_fahrenheit(temp_in_kelvin):
    """
    Convert temperature from Kelvin to Fahrenheit.
    """
    return (temp_in_kelvin - 273.15) * (9/5) + 32

def transform_load_data(task_instance):
    """
    Transform and load weather data to S3.
    """
    try:
        data = task_instance.xcom_pull(task_ids="extract_weather_data")
        if data is None:
            raise ValueError("No data found in XCom")

        city = data["name"]
        weather_description = data["weather"][0]['description']
        temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp"])
        feels_like_fahrenheit = kelvin_to_fahrenheit(data["main"]["feels_like"])
        min_temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
        max_temp_fahrenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
        pressure = data["main"]["pressure"]
        humidity = data["main"]["humidity"]
        wind_speed = data["wind"]["speed"]
        time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
        sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
        sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

        transformed_data = {
            "City": city,
            "Description": weather_description,
            "Temperature (F)": temp_fahrenheit,
            "Feels Like (F)": feels_like_fahrenheit,
            "Minimum Temp (F)": min_temp_fahrenheit,
            "Maximum Temp (F)": max_temp_fahrenheit,
            "Pressure": pressure,
            "Humidity": humidity,
            "Wind Speed": wind_speed,
            "Time of Record": time_of_record,
            "Sunrise (Local Time)": sunrise_time,
            "Sunset (Local Time)": sunset_time
        }

        df_data = pd.DataFrame([transformed_data])
        aws_credentials = {
            "key": "{{ var.value.aws_key }}",
            "secret": "{{ var.value.aws_secret }}",
            "token": "{{ var.value.aws_token }}"
        }

        now = datetime.now()
        dt_string = now.strftime("%Y%m%d%H%M%S")
        file_name = f"current_weather_data_portland_{dt_string}.csv"
        s3_path = f"s3://{{ var.value.s3_bucket_name }}/{file_name}"

        df_data.to_csv(s3_path, index=False, storage_options=aws_credentials)
        logging.info(f"Data successfully written to {s3_path}")

    except Exception as e:
        logging.error(f"Error in transform_load_data: {str(e)}")
        raise

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 8),
    'email': ['myemail@domain.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG('weather_dag',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False) as dag:

    is_weather_api_ready = HttpSensor(
        task_id='is_weather_api_ready',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=Portland&APPID=5031cde3d1a8b9469fd47e998d7aef79'
    )

    extract_weather_data = SimpleHttpOperator(
        task_id='extract_weather_data',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=Portland&APPID=5031cde3d1a8b9469fd47e998d7aef79',
        method='GET',
        response_filter=lambda r: json.loads(r.text),
        log_response=True
    )

    transform_load_weather_data = PythonOperator(
        task_id='transform_load_weather_data',
        python_callable=transform_load_data
    )

    is_weather_api_ready >> extract_weather_data >> transform_load_weather_data
