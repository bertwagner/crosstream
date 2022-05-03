
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

# Add predicate pushdown
# write actual tests, move this code to README examples
from streaming_data_joiner.data_types import CSVData, QueryData

path = Path(__file__).resolve().parents[0]

file1 = os.path.join(path,'small_data_1.csv')
file2 = os.path.join(path,'small_data_2.csv')


##########################

import csv

c1=CSVData(file1,True,[0,1])
c2=CSVData(file2, True, ['col1','col2'])

h=HashJoin()


with open('joined_data.csv', 'w') as f:
    w =csv.writer(f)
    
    # write header column names
    w.writerow(c1.column_names + c2.column_names)

    for row_left,row_right in h.inner_join(c1,c2):
        # write matched results
        w.writerow(row_left + row_right)

connection_string2 = f'Driver=SQLite3;Database={path}/example.db'
query2 = 'SELECT Col1,Col2 FROM OneMTestData'



file1 = os.path.join(path, 'small_data_1.csv')

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

def custom_join_key(row,indices):
    # calculate the hash of join values
    join_values = []
    for col_index in indices:
        join_values.append(str(row[col_index]))
    join_key = 'AAA'.join(join_values)

    return join_key

h=HashJoin()
h.inner_join(c1,c2,custom_join_key)
h.inner_join(c1,q2,custom_join_key)

def custom_process_matched_hashes(bucket_row,probe_row, bucket_join_column_indexes, probe_join_column_indexes):
        print("WOO!", bucket_row,probe_row)

h=HashJoin()
h.inner_join(c1,c2,custom_join_key,custom_process_matched_hashes)
h.inner_join(c1,q2,custom_join_key,custom_process_matched_hashes)