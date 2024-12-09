from requests import *
import json
import pandas as pd
from dotenv import *
import numpy as np 
from datetime import timedelta, datetime, date
import os
import sys

from mysqlconnection.connection import *

listing = pd.read_csv("Cryptolisting.csv", header=0)

print(listing.head(5))

now = datetime.now()
new_time = now - timedelta(days=735)
print(new_time)
print(type(new_time))

new_time_unix = int(datetime.timestamp(new_time) * 1000)
print(new_time_unix)
print(type(new_time_unix))

end = str(int(datetime.timestamp(now) * 1000))
start = str(new_time_unix)

liste = list(listing["id"])
print(liste)

df = pd.DataFrame() 

for i in liste:
    url = "https://api.coincap.io/v2/assets/{coin}/history?interval=d1&end={end}&start={start}".format(coin=i, end=end, start=start)
    response = get(url)
    data = response.json()["data"]
    df1 = pd.DataFrame(data)
    df1["id"] = i
    df = pd.concat([df, df1])

df.to_csv("historical_prices.csv", index=False)

load_dotenv()

user = os.getenv("user")
password = os.getenv("password")
DB = os.getenv("database")
host = os.getenv("host")
port = os.getenv("port")

print([DB, host])

connections = connection(host=host, database=DB, user=user, password=password)

drop_table = """DROP TABLE IF EXISTS `raw_prices_data`"""

curse(connections, drop_table)

sql = """CREATE TABLE IF NOT EXISTS `raw_prices_data` (
    `priceUsd` float(5),
    `time` varchar(15),
    `date` varchar(35),
    `id` varchar(30) NOT NULL
)"""

curse(connections, sql)

sql2 = """INSERT INTO `raw_prices_data` (`priceUsd`, `time`, `date`, `id`)
    VALUES (%s, %s, %s, %s)
"""

headers = df.values.tolist()

cursemany(connections, sql2, headers)

