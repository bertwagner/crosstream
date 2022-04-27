from collections import defaultdict
import csv
from typing import overload, Union
import pyodbc

import streaming_data_joiner.data_types as dt

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

    data_type = Union[dt.CSVData,dt.QueryData]
    def inner_join(self, input_1: data_type, input_2: data_type):
        """Join two datasets.

        The smaller dataset should be passed into input_1.
        """
        self.hash_buckets = defaultdict(list)

        for row in input_1.nextrow():
            self.__build_hash_input(row,input_1.join_column_indexes)

        for row in input_2.nextrow():
            join_key = self.__build_join_key(row,input_2.join_column_indexes)
            for bucket_row in self.hash_buckets[join_key]:
                print(bucket_row,row)