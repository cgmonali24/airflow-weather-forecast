from datetime import datetime
import requests
from airflow.models import Variable




def fetch_data( api_key,api_base_url):
    """
    Fetches weather data from the given API and returns a list of weather information.
    Args:
        api_key (str): The API key for authentication.
        api_base_url (str): The base URL of the weather API.
    Returns:
        list: A list of dictionaries containing weather information. Each dictionary contains:
    """
    req_url = f'{api_base_url}&appid={api_key}'
    response = requests.get(req_url)
    data = response.json()
    weather_data=[]

    for data in data['list']:
            dt_obj = datetime.fromtimestamp(data['dt'])
            date=dt_obj.date()
            weather_info = {
                'dt': data['dt'],
                'date': date.isoformat(),
                'min_temperature': data['main']['temp_min'],
                'max_temperature': data['main']['temp_max'],
                'humidity': data['main']['humidity'],
                'description': data['weather'][0]['description'],
                'wind_speed': data['wind']['speed']
            }
            weather_data.append(weather_info)
   
    return weather_data