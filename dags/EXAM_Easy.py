# HARSH SAHU
# SHIYAMA Suntharalingam

# We have included 5 files. Main, Easy, Medium, Hard and read_api file. Please check all of them !

import json
from datetime import datetime
import requests
from airflow.decorators import dag, task

@dag(
    start_date = datetime(2023, 1, 11),
    schedule_interval = "0 1 * * *",
    catchup = False
)
def EXAM_Easy():
    URL = "https://opensky-network.org/api/flights/departure?airport=LFPG&begin=1669852800&end=1669939200"

    @task(multiple_outputs=True)
    def read_flights() -> dict:
        r = requests.get(URL)
        flights = r.json()
        flightsString = json.dumps(flights)
        print(flightsString)
        return {"flights": flights}
        with open('./dags/EXAM_Easy_OUTPUT_flights.json', "w") as f:
            json.dump(flights["flights"],f)
    @task
    def write_toJSON(flights: dict) -> None:
        with open('./dags/EXAM_Easy_OUTPUT_flights.json', "w") as f:
            json.dump(flights["flights"],f)



    flights = read_flights()
    write_toJSON(flights)

_ = EXAM_Easy()