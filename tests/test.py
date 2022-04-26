
import os
from pathlib import Path
import sys, os

PROJECT_PATH = os.getcwd()
SOURCE_PATH = os.path.join(
    PROJECT_PATH,"src"
)
sys.path.append(SOURCE_PATH)
from streaming_data_joiner.hash_join import HashJoin

#https://bertwagner.com/posts/hash-match-join-internals/
#https://rosettacode.org/wiki/Hash_join
#https://github.com/dschwertfeger/partitioned-hash-join

# stream csv
# stream pyodbc

# allow different method signatures
# create datareader class to standardize input stream regardless of file or pyodbc, handle headers


# join csv to csv
# join csv to pyodbc
# join pyodbc to pydobc

# inner hash join
# left looping join

# join columns by name
# join columns by index

# profile code for performance
# multithreaded?
# might have to spill hash buckets/results to disk
# output to pandas dataframe? output to csv?

#case insensitive joins
#override equality method (to allow for match codes)


path = Path(__file__).resolve().parents[0]
print(path)
file1 = os.path.join(path,'small_data_1.csv')
file2 = os.path.join(path,'small_data_2.csv')

h = HashJoin()
h.inner_join(file1,True,[0,1],file2,True,[0,1])



connection_string2 = f'Driver=SQLite3;Database={path}/example.db'
query2 = 'SELECT Col1,Col2 FROM OneMTestData'

# import pyodbc
# cnxn = pyodbc.connect(connection_string2)
# cursor = cnxn.cursor()
# for i in range(100000):
#     cursor.execute("INSERT INTO OneMTestData (Col1) VALUES ('a')")
#     cnxn.commit()


h.inner_join_csv_pyodbc(file1,True,[0,1],connection_string2,query2,[0,1])