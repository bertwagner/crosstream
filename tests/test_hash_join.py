import pytest

import crosstream as cs
from crosstream.hash_join import HashJoin
import os, csv

def test_csv_false_headers(csv_input_data):
    with pytest.raises(ValueError):
        c1=cs.read_csv(csv_input_data[0],False,['col1','col2']) 
    
def test_csv_mixed_headers(csv_input_data):
    with pytest.raises(ValueError):
        c1=cs.read_csv(csv_input_data[0],True,[1,'col2']) 

def test_odbc_mixed_headers(sqlite_input_data):
    with pytest.raises(ValueError):
        q1=cs.read_odbc(sqlite_input_data,'SELECT * from test_data',[1,'col2'])

def test_csv_to_csv(tmp_path_factory,csv_input_data):
    # columns referred to by index and name
    c1=cs.read_csv(csv_input_data[0],True,[0,1])
    c2=cs.read_csv(csv_input_data[1], True, ['col1','col2'])

    h=HashJoin()
    
    output_file = os.path.join(tmp_path_factory.getbasetemp(),'csv_to_csv_output.csv')

    with open(output_file, 'w', newline='') as f:
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
a1,1,a1,1
a1,1,a1,1
'''

def test_csv_to_odbc(tmp_path_factory,csv_input_data,sqlite_input_data):

    # columns referred to by index and name
    c1=cs.read_csv(csv_input_data[0],True,[0,1])
    q1=cs.read_odbc(sqlite_input_data,'SELECT * from test_data',['col1','col2'])

    output_file = os.path.join(tmp_path_factory.getbasetemp(),'csv_to_odbc_output.csv')

    with open(output_file, 'w', newline='') as f:
        w =csv.writer(f)
        
        # write header column names
        w.writerow(c1.column_names + q1.column_names)

        for row_left,row_right in cs.inner_hash_join(c1,q1):
            # write matched results
            w.writerow(row_left + row_right)

    with open(output_file, 'r') as f:
        file_content = f.read()

        assert file_content == '''col1,col2,col1,col2
a,1,a,1
a,3,a,3
b,1,b,1
c,3,c,3
a1,1,a1,1
a1,1,a1,1
'''

def test_odbc_to_odbc(tmp_path_factory,sqlite_input_data):

    # columns referred to by index and name
    q1=cs.read_odbc(sqlite_input_data,'SELECT * from test_data',[0])
    q2=cs.read_odbc(sqlite_input_data,'SELECT col2,col1 from test_data',[1])

    output_file = os.path.join(tmp_path_factory.getbasetemp(),'odbc_to_odbc_output.csv')

    with open(output_file, 'w', newline='') as f:
        w =csv.writer(f)
        
        # write header column names
        w.writerow(q1.column_names + q2.column_names)

        for row_left,row_right in cs.inner_hash_join(q1,q2):
            # write matched results
            w.writerow(row_left + row_right)

    with open(output_file, 'r') as f:
        file_content = f.read()
        print(file_content)
        assert file_content == '''col1,col2,col2,col1
a,1,1,a
a,3,1,a
a,1,3,a
a,3,3,a
b,1,1,b
c,3,3,c
d,1,1,d
d,2,1,d
d,1,2,d
d,2,2,d
a1,1,1,a1
'''

def test_custom_overrides(tmp_path_factory,csv_input_data,sqlite_input_data):

    # columns referred to by index and name
    c1=cs.read_csv(csv_input_data[0],True,[0,1])
    c2=cs.read_csv(csv_input_data[1], True, ['col1','col2'])

    # define a function for transforming join key data before it's hashed
    def custom_join_key_transform(value):
        transformed = value.replace(' ','')
        return transformed

    # define a function for performing additional transformations or adding additional outputs before the columns are returned
    def custom_process_matched_hashes(bucket_row,probe_row, bucket_join_column_indexes, probe_join_column_indexes):
        # adding a new column indicating the weights of these matches are equal to 1
        weight=1.0
        return tuple(bucket_row),tuple(probe_row),(weight,)

    output_file = os.path.join(tmp_path_factory.getbasetemp(),'csv_to_odbc_overrides_output.csv')

    with open(output_file, 'w', newline='') as f:
        w =csv.writer(f)
        
        # write header column names
        w.writerow(c1.column_names + c2.column_names + ['weight'])

        for row_left,row_right,weight in cs.inner_hash_join(c1,c2,override_join_key_transform=custom_join_key_transform,override_process_matched_hashes=custom_process_matched_hashes):
            # write matched results
            w.writerow(row_left + row_right + weight)

    with open(output_file, 'r') as f:
        file_content = f.read()

        assert file_content == '''col1,col2,col1,col2,weight
a,1,a,1,1.0
a,3,a,3,1.0
b,1,b,1,1.0
c,3,c,3,1.0
d e,1,de,1,1.0
a1,1,a1,1,1.0
a1,1,a1,1,1.0
'''

if __name__ == "__main__":
    pytest.main(["tests/test_hash_join.py", "-s", "-k", "test_custom_overrides"])

