import pytest


class TestHashJoin:

    def test_one(self):
        self.value = 1
        assert self.value == 1

    def test_two(self):
        self.value = 1
        assert self.value == 2


from data_joiner import hash_join as hj, data_types as dt
import csv, os
from pathlib import Path

path = Path(__file__).resolve().parents[0]

file1 = os.path.join(path,'small_data_1.csv')
file2 = os.path.join(path,'small_data_2.csv')

c1=dt.CSVData(file1,True,[0,1])
c2=dt.CSVData(file2, True, ['col1','col2'])

h=hj.HashJoin()


with open('joined_data.csv', 'w') as f:
    w =csv.writer(f)
    
    # write header column names
    w.writerow(c1.column_names + c2.column_names)

    for row_left,row_right in h.inner_join(c1,c2):
        # write matched results
        w.writerow(row_left + row_right)


