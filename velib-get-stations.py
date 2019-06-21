import json
import time
import urllib.request

from kafka import KafkaProducer

API_KEY = "b4b02f9db721ef32bf28bb474e218ec9be3b55c6" # FIXME Set your own API key here
url = "https://api.jcdecaux.com/vls/v1/stations?apiKey={}".format(API_KEY)

producer = KafkaProducer(bootstrap_servers="localhost:9092")

while True:
    response = urllib.request.urlopen(url)
    stations = json.loads(response.read().decode())
    for station in stations:
        producer.send("velib-stations", json.dumps(station).encode(),
                      key=str(station["number"]).encode())
    print("{} Produced {} station records".format(time.time(), len(stations)))
    time.sleep(1)