
import json
from airflow.hooks.postgres_hook import PostgresHook




def insert_weather_data(postgres_conn_id,**kwargs):
    weather_data_str = kwargs['ti'].xcom_pull(task_ids='process_data', key='processed_data')  
    weather_data = json.loads(weather_data_str) 
    conn=PostgresHook(postgres_conn_id).get_conn()
    cur=conn.cursor()
    insert_query = """
        INSERT INTO weather_data_description (dt, date, time, min_temperature, max_temperature, average_temperature, avg_temp_celsius, temp_change, humidity, wind_speed, wind_category, description)
        VALUES (TO_TIMESTAMP(%s), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (dt) DO UPDATE SET
            date = EXCLUDED.date,
            time = EXCLUDED.time,
            min_temperature = EXCLUDED.min_temperature,
            max_temperature = EXCLUDED.max_temperature,
            average_temperature = EXCLUDED.average_temperature,
            avg_temp_celsius = EXCLUDED.avg_temp_celsius,
            temp_change = EXCLUDED.temp_change,
            humidity = EXCLUDED.humidity,
            wind_speed = EXCLUDED.wind_speed,
            wind_category = EXCLUDED.wind_category,
            description = EXCLUDED.description;
        """
        
    data_to_insert = [
        (data['dt'], data['date'], data['time'], data['min_temperature'], data['max_temperature'], data['average_temperature'], data['average_temperature_celsius'], data['temp_change'], data['humidity'], data['wind_speed'], data['wind_category'], data['description'])
        for data in weather_data
    ]

    cur.executemany(insert_query, data_to_insert)
    conn.commit()
    cur.close()
    conn.close()