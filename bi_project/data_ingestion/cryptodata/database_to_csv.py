import pandas as pd
import json
import sqlalchemy
from mysqlconnection import connection

from mysql.connector import *
import os
import sys
from dotenv import *

load_dotenv()

host = os.getenv("host")
username = os.getenv("user")
port = os.getenv("port")
password = os.getenv("password")
DB = "fct_tables_revenues"

connections = connection.connection(host, DB, username, password)
db_cursor = connections.cursor()
sql = """SELECT * FROM `fct_prices_data`"""

db_cursor.execute(sql)
tables = db_cursor.fetchall()

connections.close()

df = pd.DataFrame(tables, columns=["id", "new_date", "y0", "y1", "y2", "y3"])
print(df.head(10))

df.to_csv("fct_prices_1.csv", index=False)
