import pytest
import os, csv
import sqlite3

@pytest.fixture(scope='session')
def csv_input_data(tmp_path_factory):
    file1_headers = ['col1', 'col2']
  
    file1_data = [
    {'col1': 'a', 'col2': 1},
    {'col1': 'a', 'col2': 2},
    {'col1': 'a', 'col2': 3},
    {'col1': 'b', 'col2': 1},
    {'col1': 'b', 'col2': 2},
    {'col1': 'b', 'col2': 3},
    {'col1': 'b', 'col2': 4},
    {'col1': 'c', 'col2': 2},
    {'col1': 'c', 'col2': 3}
    ]

    output1 = os.path.join(tmp_path_factory.getbasetemp(),'test_data_1.csv')
  
    with open(output1, 'w') as f:
        writer = csv.DictWriter(f, fieldnames = file1_headers)
        writer.writeheader()
        writer.writerows(file1_data)
    
    file2_headers = ['col1', 'col2']
  
    file2_data = [
    {'col1': 'a', 'col2': 1},
    {'col1': 'a', 'col2': 3},
    {'col1': 'b', 'col2': 1},
    {'col1': 'c', 'col2': 3},
    {'col1': 'd', 'col2': 1},
    {'col1': 'd', 'col2': 2}
    ]

    output2 = os.path.join(tmp_path_factory.getbasetemp(),'test_data_2.csv')
  
    with open(output2, 'w') as f:
        writer = csv.DictWriter(f, fieldnames = file2_headers)
        writer.writeheader()
        writer.writerows(file2_data)

    return [output1,output2]

@pytest.fixture(scope='session')
def sqlite_input_data(tmp_path_factory):
    db_path = os.path.join(tmp_path_factory.getbasetemp(),'example.db')
    con = sqlite3.connect(db_path)

    connection_string = f'Driver=SQLite3;Database={db_path}'

    return connection_string