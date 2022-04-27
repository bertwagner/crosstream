import csv
from typing import List, overload
import pyodbc

class CSVData():
    @overload
    def __init__(self, csv_file_path: str, has_headers: bool, join_columns: List[str]):
        ...
    
    @overload
    def __init__(self, csv_file_path: str, has_headers: bool, join_columns: List[int]):
        ...

    def __init__(self, csv_file_path, has_headers, join_columns):
        self.csv_file_path = csv_file_path
        self.has_headers=has_headers

        if isinstance(join_columns, list) and all(isinstance(x, int) for x in join_columns):
            self.join_column_indexes = join_columns

        elif isinstance(join_columns, list) and all(isinstance(x, str) for x in join_columns):
            if has_headers==False:
                raise ValueError('has_headers=False, but column names passed to join_columns')

            # determine column indexes from names
            with open(csv_file_path) as f:
                reader = csv.reader(f)
                headers = next(reader)
                join_column_indexes=[]

                for column_name in join_columns:
                    column_index = headers.index(column_name)
                    join_column_indexes.append(column_index)
            
            self.join_column_indexes = join_column_indexes
        else:
            raise ValueError('join_columns must be of type List[int] or List[str]')
    
    def nextrow(self):
        with open(self.csv_file_path) as f:
            reader = csv.reader(f)

            # skip reading in the header row
            if self.has_headers:
                next(reader)

            for row in reader:
                yield tuple(row)
    
class QueryData():
    @overload
    def __init__(self, connection_string: str, query: str, join_columns: List[str]):
        ...
    
    @overload
    def __init__(self, connection_string: str, query: str, join_columns: List[int]):
        ...

    def __init__(self, connection_string, query, join_columns):
        self.connection_string = connection_string
        self.query = query

        if isinstance(join_columns, list) and all(isinstance(x, int) for x in join_columns):
            self.join_column_indexes = join_columns

        elif isinstance(join_columns, list) and all(isinstance(x, str) for x in join_columns):
            # determine column indexes from names
            cnxn = pyodbc.connect(self.connection_string)
            cursor = cnxn.cursor()
            cursor.execute(query)
            join_column_indexes=[]
            columns = [column[0] for column in cursor.description]
            
            for column_name in join_columns:
                column_index = columns.index(column_name)
                join_column_indexes.append(column_index)
            
            self.join_column_indexes = join_column_indexes
        else:
            raise ValueError('Must be of type List[int] or List[str]')
    
    def nextrow(self):
        connection = pyodbc.connect(self.connection_string)
        with connection:
            cursor = connection.cursor()
            cursor.execute(self.query)
            row = cursor.fetchone() 
            while row: 
                yield row
                row = cursor.fetchone()