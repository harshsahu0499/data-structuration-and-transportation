from datetime import datetime, date, timedelta
from time import mktime

import requests


def read_from_api():
    BASE_URL = "https://api.open-meteo.com/"
    params = {
        "latitude": "48.85",
        "longitude": "2.35",
        "hourly": "temperature_2m"
    }


    meteo_forecast = f"{BASE_URL}/v1/forecast"
    response = requests.get(meteo_forecast, params=params)

    request = response.json();
    return request


def to_seconds_since_epoch(input_date: str) -> int:
    return int(mktime(date.fromisoformat(input_date).timetuple()))