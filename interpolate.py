#!/usr/bin/python3

# import sys
# print(sys.getsizeof(""))

import psycopg2
from psycopg2.extras import execute_values
from multiprocessing import Pool

SIZE = 5
PROC_COUNT = 32


def main():
    test_db_connection()
    initialize_db()

    pi_data = split_data(get_pi_data(), PROC_COUNT)

    with Pool(PROC_COUNT) as p:
        p.map(process_data, pi_data)

        # for segment in pi_data:
        #     process_data(segment)


"""
Parsing and manipulation functions
"""


def get_pi_data():
    # Get that data
    f = open("pi-billion.txt", "r")
    pi_data = f.read()
    f.close()

    return pi_data


def split_data(arr, parts):
    length = len(arr)
    offset = SIZE-1
    return [arr[i*length // parts: ((i+1)*length // parts)+offset] for i in range(parts)]


def process_data(segment):
    conn = connect_to_db()
    cur = conn.cursor()

    subarr = list()
    argslist = list()

    print("processing...")

    for c in segment:
        subarr.append(c)

        # Skip publishing when still initializing
        if len(subarr) == SIZE:
            try:
                argslist.append([''.join(subarr)])
                subarr.pop(0)
            except (Exception, psycopg2.errors.UniqueViolation):
                pass

            if len(argslist) > 1000:
                execute_values(
                    cur, "INSERT INTO pi_values (number) VALUES %s", argslist)
                conn.commit()
                argslist.clear()

    cur.close()
    conn.close()


class Tree():
    def __init__(self):
        self._nodelist = [None] * 10  # base 10 value

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def next(self):
        """
        Recursive generator.
        Yields stored values; ordered from smallest to largest.
        """
        # Check all nodes in the list
        for i, node in enumerate(self._nodelist):
            if node:
                # Continue if there is data downstream
                while val := node.next():
                    yield i + val

        # No values left
        yield None

    def insert(self, argslist):
        val = argslist.pop(0)
        node = self._nodelist[val]

        if argslist:
            if node is None:
                node = Tree()
                self._nodelist[val] = node

            node.update(argslist)


"""
SQLLite functions
"""


def connect_to_db():

    dbhost = "localhost"
    pguser = "localadmin"
    pgpass = "X77i&%Hs$6JC*etr5F8!"
    dbname = "test_db"
    conn = psycopg2.connect(
        host=dbhost, dbname=dbname, user=pguser, password=pgpass)

    return conn


def test_db_connection():

    conn = connect_to_db()
    cur = conn.cursor()

    # Execute a test statement
    # print('PostgreSQL database version:')
    cur.execute('SELECT version()')
    # print(cur.fetchone())
    cur.close()


def initialize_db():
    conn = connect_to_db()
    cur = conn.cursor()
    # cur.execute(
    #     "CREATE TABLE IF NOT EXISTS pi_values (number TEXT UNIQUE, figures VARCHAR (50))")
    cur.execute(
        "CREATE TABLE IF NOT EXISTS pi_values (number TEXT)")
    conn.commit()

    cur.close()
    conn.close()


if __name__ == "main":
    main()
