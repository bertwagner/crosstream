
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

# TODO:
# Load from dataframe
# output to dataframe (with headers)
# output to csv (with headers)

# override equality method (to allow for match codes)

# left looping join

# profile code for performance
# multithreaded?
# might have to spill hash buckets/results to disk
# output to pandas dataframe? output to csv?




path = Path(__file__).resolve().parents[0]
print(path)
file1 = os.path.join(path,'small_data_1.csv')
file2 = os.path.join(path,'small_data_2.csv')

h = HashJoin()
#h.inner_join(file1,True,[0,1],file2,True,[0,1])


connection_string2 = f'Driver=SQLite3;Database={path}/example.db'
query2 = 'SELECT Col1,Col2 FROM OneMTestData'


#h.inner_join_csv_pyodbc(file1,True,[0,1],connection_string2,query2,[0,1])


from streaming_data_joiner.data_types import CSVData, QueryData

file1 = os.path.join(path, 'small_data_1.csv')

c1=CSVData(file1,True,[0,1])

c2=CSVData(file2, True, ['col1','col2'])

for row in c1.nextrow():
    pass


connection_string2 = f'Driver=SQLite3;Database={path}/example.db'
query2 = 'SELECT Col2,Col1 FROM OneMTestData'
q1 = QueryData(connection_string2,query2,[0])
q2 = QueryData(connection_string2,query2,['Col1','Col2'])

for row in q1.nextrow():
    pass

pass

h=HashJoin()
h.inner_join(c1,c2)
h.inner_join(c1,q2)
