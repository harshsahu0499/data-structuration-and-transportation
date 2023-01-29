# HARSH SAHU
# SHIYAMA Suntharalingam


import json
from datetime import datetime
import requests
from airflow.decorators import dag, task

@dag(
    schedule=None,
    start_date=datetime(2023, 1, 11),
    catchup=False
)
def EXAM_Main():
    @task
    def read_flights(link: str):
        r = requests.get(link)
        flights = r.json()
        flightsString = json.dumps(flights)
        print(flightsString)
        return flightsString

    @task
    def write_toJSON(flights):
        flights_JSON = json.loads(flights)
        with open('./dags/EXAM_Main_OUTPUT_flights.json', "w") as f:
            json.dump(flights_JSON,f)



    flights = read_flights("https://opensky-network.org/api/flights/departure?airport=LFPG&begin=1669852800&end=1669939200")
    write_toJSON(flights)

_ = EXAM_Main()