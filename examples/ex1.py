#!/usr/bin/env python
import pandas as pd
import numpy as np
import odo
import profilehooks
import sqlalchemy
import csv
import os


def create_test_data():
    n = 100000
    df = pd.DataFrame(dict(
        id=np.random.randint(0, 1000000, n),
        col1=np.random.choice(['hello', 'world', 'python', 'large string for testing ' * 10], n),
        col2=np.random.randint(-1000000, 1000000, n),
        col3=np.random.randint(-9000000, 9000000, n),
        col4=(np.random.random(n) - 0.5) * 99999
    ), columns=['id', 'col1', 'col2', 'col3', 'col4'])
    df.to_csv('tmp.csv', index=False)


@profilehooks.timecall
def using_pandas(table_name, uri):
    df = pd.read_csv('tmp.csv')
    df.to_sql(table_name, con=uri, if_exists='append', index=False)


@profilehooks.timecall
def using_odo(table_name, uri):
    odo.odo('tmp.csv', '%s::%s' % (uri, table_name))


@profilehooks.timecall
def using_cursor(table_name, uri):
    engine = sqlalchemy.create_engine(uri)
    query = 'INSERT INTO {} (id, col1, col2, col3, col4) VALUES(%s, %s, %s, %s, %s)'
    query = query.format(table_name)
    con = engine.raw_connection()
    with con.cursor() as cursor:
        with open('tmp.csv') as fh:
            reader = csv.reader(fh)
            next(reader)  # Skip firt line (headers)
            for row in reader:
                cursor.execute(query, row)
                con.commit()
    con.close()


@profilehooks.timecall
def using_cursor_correct(table_name, uri):
    engine = sqlalchemy.create_engine(uri)
    query = 'INSERT INTO {} (id, col1, col2, col3, col4) VALUES(%s, %s, %s, %s, %s)'
    query = query.format(table_name)
    with open('tmp.csv') as fh:
        reader = csv.reader(fh)
        next(reader)  # Skip firt line (headers)
        data = list(reader)
    engine.execute(query, data)


def main():
    uri = 'mysql+pymysql://root:%s@localhost/test' % os.environ['pass']

    engine = sqlalchemy.create_engine(uri)
    for i in (1, 2, 3, 4):
        engine.execute("DROP TABLE IF EXISTS table%s" % i)
        engine.execute("""
            CREATE TABLE table%s(
                id INT,
                col1 VARCHAR(255),
                col2 INT,
                col3 INT,
                col4 DOUBLE
            );
        """ % i)
    create_test_data()

    using_odo('table1', uri)
    using_pandas('table4', uri)
    using_cursor_correct('table3', uri)
    using_cursor('table2', uri)

    for i in (1, 2, 3, 4):
        count = pd.read_sql('SELECT COUNT(*) as c FROM table%s' % i, con=uri)['c'][0]
        print("Count for table%s - %s" % (i, count))


if __name__ == '__main__':
    main()