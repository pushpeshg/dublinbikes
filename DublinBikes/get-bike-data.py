# Author     : Pushpesh Gupta
# Description: This script streams Twitter data into Kinesis Firehose. Used for Onboarding, Phase 5 exercise.

# Import modules
import requests
import json
from datetime import datetime
import boto3
from _datetime import date

# Firehose stream name
deliveryStreamName = "dubln-bikes"

# Dublin Bike Key
apiKey = "c8c008f4c75dfe0a49a57c595ea90935d872cdc5"
file_name = "Dublin.json"
contract = "Dublin"


def stations_list(file_name):
    data = json.load(open(file_name))
    stations = []
    for i in data:
        stations.append(i["number"])
        stations.sort()
    return stations


def query_api(station_no):
    r = requests.get("https://api.jcdecaux.com/vls/v1/stations/" + str(station_no) + "?contract=" + contract + "&apiKey=" + apiKey)
    r = r.json()
    return r

def single_station_info(station_no):
    op = query_api(station_no)
    dt = datetime.now().date()
    tm = datetime.now().time()

    station_info = {
                    'number': op["number"],
                    'name': op["name"],
                    'latitude': op["position"]["lat"],
                    'longitude': op["position"]["lng"],
                    'bikes': op["available_bikes"],
                    'stands': op["available_bike_stands"],
                    'time': tm,
                    'date': dt
                    }
    return station_info

# test_stn = stations_list(fileName)
# one = test_stn[1]

# output = single_station_info(3)

# print(output)


# Setup Firehose
client = boto3.client('firehose', region_name='us-east-1')

lists = stations_list(file_name)

for i in lists:
    try:
        output = single_station_info(i)
        response = client.put_record(
            DeliveryStreamName=deliveryStreamName,
            Record={
                'Data': str(output) + "\n"
            }
        )
        print(response)
    except Exception as e:
        print(e)
        pass

