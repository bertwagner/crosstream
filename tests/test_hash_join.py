import pytest
from confluent.hash_join import HashJoin
from confluent.data_types import CSVData,QueryData
import os, csv


def test_csv_to_csv(tmp_path_factory,csv_input_data):
    # columns referred to by index and name
    c1=CSVData(csv_input_data[0],True,[0,1])
    c2=CSVData(csv_input_data[1], True, ['col1','col2'])

    h=HashJoin()

    output_file = os.path.join(tmp_path_factory.getbasetemp(),'csv_to_csv_output.csv')

    with open(output_file, 'w') as f:
        w =csv.writer(f)
        
        # write header column names
        w.writerow(c1.column_names + c2.column_names)

        for row_left,row_right in h.inner_join(c1,c2):
            # write matched results
            w.writerow(row_left + row_right)

    with open(output_file, 'r') as f:
        file_content = f.read()
        assert file_content == '''col1,col2,col1,col2
a,1,a,1
a,3,a,3
b,1,b,1
c,3,c,3
'''

def test_csv_to_odbc(tmp_path_factory,csv_input_data,sqlite_input_data):

    # columns referred to by index and name
    c1=CSVData(csv_input_data[0],True,[0,1])
    q1=QueryData(sqlite_input_data,'SELECT 1 as test',[0])

    assert 1==1


# odbc to odbc
# hash join
# loop join
# custom override functions
# output more values in override functions
