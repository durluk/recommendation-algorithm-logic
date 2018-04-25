import psycopg2

from dataAccess.config import config

connection = None


def connect():
    """ Connect to the PostgreSQL database server """
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        connection = psycopg2.connect(**params)
        getDbParameters(connection)
        return connection

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def getDbParameters(conn):
    # create a cursor
    cur = conn.cursor()
    # execute a statement
    print('PostgreSQL database version:')
    cur.execute('SELECT version()')
    # display the PostgreSQL database server version
    db_version = cur.fetchone()
    print(db_version)
    # close the communication with the PostgreSQL
    cur.close()


def close():
    if connection is not None:
        connection.close()
        print('Database connection closed.')
