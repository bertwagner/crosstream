from collections import defaultdict
import csv
import pyodbc

class HashJoin:
    def __init__(self):
        self.hash_buckets = defaultdict(list)

    def __build_hash_input(self, row, join_column_indexes):
        join_key = self.__build_join_key(row,join_column_indexes)
        self.hash_buckets[join_key].append(row)

    def __build_join_key(self,row,indices):
        # calculate the hash of join values
        join_values = []
        for col_index in indices:
            join_values.append(str(row[col_index]))
        join_key = ''.join(join_values)

        return join_key

    def inner_join(self, file_1, contains_headers_1, join_indexes_1, file_2, contains_headers_2, join_indexes_2):
        self.hash_buckets = defaultdict(list)
        # load first file into hash buckets
        # ideally the first file is the smaller dataset
        with open(file_1) as f:
            reader = csv.reader(f)
            if contains_headers_1:
                all_join_indexes_numeric = all([isinstance(item,int) for item in join_indexes_1])
                if all_join_indexes_numeric:
                    next(reader)
                else:
                    #TODO: Find numeric indices for column names
                    pass

            for row in reader:
                self.__build_hash_input(row,join_indexes_1)
       
        with open(file_2) as f:
            reader = csv.reader(f)
            if contains_headers_2:
                all_join_indexes_numeric = all([isinstance(item,int) for item in join_indexes_2])
                if all_join_indexes_numeric:
                    next(reader)
                else:
                    #TODO: Find numeric indices for column names
                    pass

            for row in reader:
                join_key = self.__build_join_key(row,join_indexes_2)
                for bucket_row in self.hash_buckets[join_key]:
                    print(bucket_row,row)

    def inner_join_csv_pyodbc(self, file_1, contains_headers_1,join_indexes_1,connection_string_2,query_2,join_indexes_2):
        self.hash_buckets = defaultdict(list)
        # load first file into hash buckets
        # ideally the first file is the smaller dataset
        with open(file_1) as f:
            reader = csv.reader(f)
            if contains_headers_1:
                all_join_indexes_numeric = all([isinstance(item,int) for item in join_indexes_1])
                if all_join_indexes_numeric:
                    next(reader)
                else:
                    #TODO: Find numeric indices for column names
                    pass

            for row in reader:
                self.__build_hash_input(row,join_indexes_1)
        
        
        all_join_indexes_numeric = all([isinstance(item,int) for item in join_indexes_2])
        
        cnxn = pyodbc.connect(connection_string_2)
        cursor = cnxn.cursor()
        cursor.execute(query_2)
        row = cursor.fetchone() 
        while row: 
            join_key = self.__build_join_key(row,join_indexes_2)
            for bucket_row in self.hash_buckets[join_key]:
                    print(bucket_row,row)
            row = cursor.fetchone()
