import pandas as pd
import json 
from mysqlconnection.connection import *
from requests import *
import sys
import os
from mysql.connector import *
from dotenv import * 
from datetime import *

## First of all we would love to get the listing of the crypto id we have in our possession

coin_data = pd.read_csv("Cryptolisting.csv")
print(coin_data.head(10))

list_coin = list(coin_data["id"])
print(list_coin)
print(type(list_coin))

## Then we will connect to the crypto api in order to retrieve what we want

# dt_end = datetime.now() - timedelta(days=1)
# dt_start = datetime.now() - timedelta(days=835)

# unix_end = str(int(datetime.timestamp(dt_end))*1000)
# unix_start = str(int(datetime.timestamp(dt_start))*1000)

df = pd.DataFrame()

for i in [0, 101]:
    j = i + 100
    url = "https://api.coinlore.net/api/tickers/?start={i}&limit={j}".format(i=i, j=j)

    headers = {
        'Content-Type': 'application/json' 
    }

    response = get(url, headers) 
    print(response.status_code)
    data = response.json()['data']
    df1 = pd.DataFrame(data)
    df = pd.concat([df, df1])
print(df.head(10))

df.to_csv('coinlore_coins.csv', index=False)

load_dotenv()

host = os.getenv("host")
user = os.getenv("user")
password = os.getenv("password")
DB = os.getenv("database")
port = os.getenv("port")

connections = connection(host, DB, user, password)

sql1 = """ DROP TABLE IF EXISTS `raw_coins_coinlore` """

curse(connections, sql1) 

sql2 = """CREATE TABLE IF NOT EXISTS `raw_coins_coinlore` (
    `id` int(15) NOT NULL,
    `symbol` varchar(30) NOT NULL,
    `name` varchar(30) NOT NULL,
    `nameid` varchar(30) NOT NULL,
    `rank` int(8) NOT NULL,
    `price_usd` float(10,4),
    `percent_change_24h` varchar(30),
    `percent_change_1h` varchar(30),
    `percent_change_7d` varchar(30),
    `price_btc` varchar(30),
    `market_cap_usd` varchar(30),
    `volume24` varchar(30),
    `volume24a` varchar(30),
    `csupply` varchar(30),
    `tsupply` varchar(30),
    `msupply` varchar(30) 
)"""

curse(connections, sql2)

add_coins = """ INSERT INTO `raw_coins_coinlore` (`id`,
    `symbol`,
    `name`,
    `nameid`,
    `rank`,
    `price_usd`,
    `percent_change_24h`,
    `percent_change_1h`,
    `percent_change_7d`,
    `price_btc`,
    `market_cap_usd`,
    `volume24`,
    `volume24a`,
    `csupply`,
    `tsupply`,
    `msupply`) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
    """

df_tolist = df.iloc[0:]
values = df_tolist.values.tolist()
print(values)

cursemany(connections, add_coins, values)
