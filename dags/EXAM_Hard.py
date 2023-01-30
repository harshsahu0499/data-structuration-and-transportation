# HARSH SAHU
# SHIYAMA Suntharalingam
import json
import sqlite3
from dataclasses import dataclass
from airflow.decorators import dag, task
from datetime import datetime, date, timedelta
import requests
from read_api import to_seconds_since_epoch


@dataclass
class Flight:
  icao24: str
  firstSeen: int
  estDepartureAirport: str
  lastSeen: int
  estArrivalAirport: str
  callsign: str
  estDepartureAirportHorizDistance: int
  estDepartureAirportVertDistance: int
  estArrivalAirportHorizDistance: int
  estArrivalAirportVertDistance: int
  departureAirportCandidatesCount: int
  arrivalAirportCandidatesCount: int

@dag(
    start_date = datetime(2023, 1, 11),
    schedule_interval = "0 1 * * *",
    catchup = False
)

def Exam_Hard():
    BASE_URL = "https://opensky-network.org/api"

    @task
    def read(ds=None):
        ds = date.today() - timedelta(days=7)
        one_day = ds + timedelta(days=1)
        params = {
            "airport": "LFPG",
            "begin": to_seconds_since_epoch(str(ds)), # using "read_api.py" to convert
            "end": to_seconds_since_epoch(str(one_day))
        }
        response = requests.get(f"{BASE_URL}/flights/departure", params=params)
        flights = response.json()
        return flights


    @task
    def writetoSQLITE(flights):
        conn = sqlite3.connect(":memory:")
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS flights (
            icao24 TEXT,
            firstSeen INTEGER,
            estDepartureAirport TEXT,
            lastSeen INTEGER,
            estArrivalAirport TEXT,
            callsign TEXT,
            estDepartureAirportHorizDistance INTEGER,
            estDepartureAirportVertDistance INTEGER,
            estArrivalAirportHorizDistance INTEGER,
            estArrivalAirportVertDistance INTEGER,
            departureAirportCandidatesCount INTEGER,
            arrivalAirportCandidatesCount INTEGER
        )
        ''')

        # storing into db
        for flight in flights:
            cursor.execute('''INSERT INTO flights (icao24, firstSeen,estDepartureAirport,lastSeen,estArrivalAirport, callsign, estDepartureAirportHorizDistance,estDepartureAirportVertDistance,estArrivalAirportHorizDistance,estArrivalAirportVertDistance,departureAirportCandidatesCount,arrivalAirportCandidatesCount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',(flight['icao24'], flight['firstSeen'], flight['estDepartureAirport'], flight['lastSeen'], flight['estArrivalAirport'], flight['callsign'], flight['estDepartureAirportHorizDistance'],flight['estDepartureAirportVertDistance'], flight['estArrivalAirportHorizDistance'],flight['estArrivalAirportVertDistance'], flight['departureAirportCandidatesCount'], flight['arrivalAirportCandidatesCount']))
        conn.commit()

        # to print
        cursor.execute("SELECT * FROM flights")
        rows = cursor.fetchall()

        for row in rows:
            print(row)

        # dropping dable
        cursor.execute('''DROP TABLE flights''')
        conn.close()


    print(writetoSQLITE(read()))


_=Exam_Hard()