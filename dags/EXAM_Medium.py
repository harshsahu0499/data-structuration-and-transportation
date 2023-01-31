# HARSH SAHU
# SHIYAMA Suntharalingam


# We have included 5 files. Main, Easy, Medium, Hard and read_api file. Please check all of them !


from airflow.decorators import dag, task
from datetime import datetime
from read_api import read_from_api

@dag(
    start_date = datetime(2023, 1, 11),
    schedule_interval = "0 1 * * *",
    catchup = False
)

def EXAM_Medium():
    @task(multiple_outputs=True)
    def read() -> dict:
        return read_from_api()  # getting data from a function in another file "read_api.py"

    @task
    def transformation(req) -> list:
        request = req
        TimeTemperature = request['hourly']
        time = TimeTemperature['time']
        temperature = TimeTemperature['temperature_2m']
        filtered = []
        for i in range(0, len(time)):
            if temperature[i] < 0.0:
                filtered.append(time[i] + " " + str(temperature[i]))

        return filtered

    @task
    # Store temperature below 0 degree to txt file
    def write_toTXT(meteoInstances):
        with open('./dags/EXAM_Medium_OUTPUT.txt', "w") as fp:
            for item in meteoInstances:
                fp.write("%s\n" % item)

    request = read()
    meteoInstances = transformation(request)
    write_toTXT(meteoInstances)

_ = EXAM_Medium()
