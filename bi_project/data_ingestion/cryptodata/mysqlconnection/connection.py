from mysql.connector import *
import sqlalchemy

def connection(host, database, user, password):
    try:
        connections = connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        print("Connecting to the database ...")
        if connections.is_connected():
            database_info = connections.get_server_info()
            print("SQL Version:" , database_info)
    
    except ValueError:
        print("We were not able to connect to the database")
    
    return connections

    
def close_connection(connection):
    closing = connection
    closing.close()

    return print("My connection has closed")

def curse(connection, sql):
    cursor = connection.cursor()
    cursor.execute(sql)

    connection.commit()


def cursemany(connection, sql, list):
    cursor = connection.cursor()
    cursor.executemany(sql, list)

    connection.commit()