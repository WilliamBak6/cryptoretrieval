import requests 
import os
import sys
import pandas as pd
import json
from pip import * 

from dotenv import *

from mysqlconnection.connection import *

url = "https://api.coincap.io/v2/assets?limit=100"

headers = {
    'Content-Type': 'application/json'
}

response = requests.get(url, headers = headers)
print(response.status_code)

Assets = response.json()
Assets = Assets["data"]
Assets = json.dumps(Assets)
Assets = json.loads(Assets)
print(type(Assets))

data = pd.DataFrame(Assets)
print(data.columns)
Data = data[["id", "rank", "symbol", "name", "marketCapUsd"]]
print(Data.head(5))

Data.to_csv("Cryptolisting.csv", index=True)

load_dotenv()

host = os.getenv("host")
username = os.getenv("user")
password = os.getenv("password")
port = os.getenv("port")
DB = os.getenv("database")

connections = connection(host, DB, username, password)

drop_table = """DROP TABLE IF EXISTS `coincap_list`"""

curse(connections, drop_table)

sql = """CREATE TABLE IF NOT EXISTS `coincap_list` (
    `name_id` varchar(50) NOT NULL, 
    `rank` int(10) NOT NULL,
    `symbol` varchar(50) NOT NULL,
    `name` varchar(50) NOT NULL,
    `marketcap` varchar(50) NOT NULL 
) """

curse(connections, sql)

add_coins = """ INSERT INTO `coincap_list` (`name_id`, `rank`, `symbol`, `name`, `marketcap`)
                VALUES (%s, %s, %s, %s, %s)
"""
headers = Data.values.tolist()

cursemany(connections, add_coins, headers)
