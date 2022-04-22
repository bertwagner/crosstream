import csv
import os
from collections import defaultdict

# stream csv
# stream pyodbc

# join csv to csv
# join csv to pyodbc
# join pyodbc to pydobc

# inner hash join
# left looping join

def hashJoin(table1, index1, table2, index2):
    h = defaultdict(list)
    # hash phase
    for s in table1:
        h[s[index1]].append(s)
    # join phase
    return [(s, r) for r in table2 for s in h[r[index2]]]
 
table1 = [(27, "Jonah"),
          (18, "Alan"),
          (28, "Glory"),
          (18, "Popeye"),
          (28, "Alan")]
table2 = [("Jonah", "Whales"),
          ("Jonah", "Spiders"),
          ("Alan", "Ghosts"),
          ("Alan", "Zombies"),
          ("Glory", "Buffy")]
 
for row in hashJoin(table1, 1, table2, 0):
    print(row)






def inner_hash_join(table1, index1, table2, index2):
    h = defaultdict(list)

    # hash phase
    for s in table1:
        h[s[index1]].append(s)

    # join phase
    return [(s, r) for r in table2 for s in h[r[index2]]]

def inner_join(csv_path_1,csv_path_2):
    with open(csv_path_1) as f:
        for row in csv.reader(f):
            print(row)

path = os.getcwd()
file1 = path+'/tests/small_data_1.csv'
file2 = path+'/tests/small_data_2.csv'

for row in hashJoin(table1, 1, table2, 0):
    print(row)


#https://gist.github.com/nickwhite917/a158a2c82d5569ce46c6a52d1aeffd55
#https://www.geeksforgeeks.org/python-hash-method/
#https://rosettacode.org/wiki/Hash_join
#https://github.com/dschwertfeger/partitioned-hash-join



