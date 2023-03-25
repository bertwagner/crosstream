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
    {'col1': 'c', 'col2': 3},
    {'col1': 'd e', 'col2': 1},
    {'col1': 'a1', 'col2': 1},
    {'col1': 'a1', 'col2': 1}
    ]

    output1 = os.path.join(tmp_path_factory.getbasetemp(),'test_data_1.csv')
  
    with open(output1, 'w', newline='') as f:
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
    {'col1': 'd', 'col2': 2},
    {'col1': 'de', 'col2': 1},
    {'col1': 'a', 'col2': 11},
    {'col1': 'a1', 'col2': 1}
    ]

    output2 = os.path.join(tmp_path_factory.getbasetemp(),'test_data_2.csv')
    with open(output2, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames = file2_headers)
        writer.writeheader()
        writer.writerows(file2_data)

    return [output1,output2]

@pytest.fixture(scope='session')
def sqlite_input_data(tmp_path_factory):
    db_path = os.path.join(tmp_path_factory.getbasetemp(),'example.db')
    # windows:
    connection_string = 'DRIVER={SQLite3 ODBC Driver};SERVER=localhost;DATABASE='+db_path+';Trusted_connection=yes'
    # linux:
    # connection_string = f'Driver=SQLite3;Database={db_path}'

    conn = sqlite3.connect(db_path)
    curs = conn.cursor()
    curs.execute('''
            CREATE TABLE test_data
            (col1 char(1), col2 int)
            ''')

    curs.execute("INSERT INTO test_data (col1,col2) VALUES ('a',1);")
    curs.execute("INSERT INTO test_data (col1,col2) VALUES ('a',3);")
    curs.execute("INSERT INTO test_data (col1,col2) VALUES ('b',1);")
    curs.execute("INSERT INTO test_data (col1,col2) VALUES ('c',3);")
    curs.execute("INSERT INTO test_data (col1,col2) VALUES ('d',1);")
    curs.execute("INSERT INTO test_data (col1,col2) VALUES ('d',2);")
    curs.execute("INSERT INTO test_data (col1,col2) VALUES ('a1',1);")

    conn.commit()
    conn.close()

    return connection_string