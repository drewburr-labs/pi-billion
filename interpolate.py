#!/usr/bin/python3

# import sys
# print(sys.getsizeof(""))

import psycopg2
import gc
from psycopg2.extras import execute_values
from multiprocessing import Pool

# Enable garbage collection
gc.enable()

SIZE = 5
PROC_COUNT = 32


def main():
    # test_db_connection()
    # initialize_db()

    data_tree = generate_tree(get_pi_data())

    # pi_data = split_data(get_pi_data(), PROC_COUNT)

    print("Data prepped..")

    print("Starting processing..")

    # with Pool(PROC_COUNT) as p:
    #     p.map(commit_data, pi_data)

    print("Processing mapped..")

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

    return pi_data[0:5000000]


def split_data(arr, parts):
    length = len(arr)
    offset = SIZE-1
    return [arr[i*length // parts: ((i+1)*length // parts)+offset] for i in range(parts)]


def commit_data(segment):
    conn = connect_to_db()
    cur = conn.cursor()

    subarr = list()
    argslist = list()

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

    if len(argslist) > 0:
        execute_values(
            cur, "INSERT INTO pi_values (number) VALUES %s", argslist)
        conn.commit()
        argslist.clear()

    cur.close()
    conn.close()


def generate_tree(pi_data):
    # Initialize subarr
    subarr = list(pi_data[0:SIZE-1])
    tree = Tree(SIZE)
    tasks = []

    for c in pi_data[SIZE-1:]:
        subarr.append(c)
        tree.insert(subarr.copy())
        subarr.pop(0)


class Tree():
    """
    A base 10 tree, with each node notating values from most to least significant.
    The end of each branch chain is a boolean value, which notates if the chain followed is a number that was inserted into the tree.
    When iterating:
        When a value is found, the boolean value at the end of the branch chain is set to False.
        If every branch on a node is found to be empty, it is set to False to avoid excessive itaration.
    """

    def __init__(self, depth):
        base = 10  # Each branch should divide into 10
        if depth > 1:
            self._nodelist = list()
            for _ in range(base):
                self._nodelist.append(Tree(depth - 1))
        elif depth == 1:
            self._nodelist = [False] * base

    def __iter__(self):
        return self

    def __next__(self):
        for i, subnode in enumerate(self._nodelist):
            # End is reached, no match
            if subnode != False:
                # End is reached, with match
                if subnode == True:
                    self._nodelist[i] = False
                    return i
                # End has not been reached
                else:
                    for val in subnode:
                        if val != None:
                            return f"{i}{val}"
                        else:
                            # No values were found
                            self._nodelist[i] = False
                            return None

        raise StopIteration

    def insert(self, data):
        """
        Responsible for inserting data into the tree.
        'data' is a list that must match the size of the tree.
        """
        current_node = self

        for item in data:
            val = int(item)
            next_node = current_node._nodelist[val]

            # When the end has been reached
            if next_node is True or next_node is False:
                current_node._nodelist[val] = True
            else:
                current_node = next_node


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


if __name__ == "__main__":
    import cProfile
    import pstats

    with cProfile.Profile() as pr:
        main()

    stats = pstats.Stats(pr)
    stats.dump_stats(filename='profile.prof')
