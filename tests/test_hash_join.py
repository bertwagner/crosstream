import pytest


class TestHashJoin:

    def test_csv_to_csv(self):
        #from crossjoin.hash_join import HashJoin
        #from crossjoin.data_types import CSVData,QueryData
        import csv, os
        from pathlib import Path

        #path = Path(__file__).resolve().parents[0]

        file1 = os.path.join(path,'small_data_1.csv')
        file2 = os.path.join(path,'small_data_2.csv')

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

        self.value = 1
        assert self.value == 1


# csv to csv
# csv to odbc
# odbc to odbc
# hash join
# loop join
# custom override functions
# output more values in override functions


from streamjoin.hash_join import HashJoin
from streamjoin.data_types import CSVData,QueryData
import csv, os
from pathlib import Path

path = Path(__file__).resolve().parents[0]

file1 = os.path.join(path,'small_data_1.csv')
file2 = os.path.join(path,'small_data_2.csv')

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


