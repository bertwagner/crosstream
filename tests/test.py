
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



path = Path(__file__).resolve().parents[0]
print(path)
file1 = os.path.join(path,'small_data_1.csv')
file2 = os.path.join(path,'small_data_2.csv')

h = HashJoin()
h.inner_join(file1,True,[0,1],file2,True,[0,1])

import pyodbc
cnxn = pyodbc.connect("Driver=SQLite3;Database=tests/example.db")
cursor = cnxn.cursor()
cursor.execute("SELECT Col1,Col2 FROM SmallTestData")
row = cursor.fetchone() 
while row: 
    print(row[1])
    row = cursor.fetchone()
