from collections import defaultdict
import csv
from typing import overload, Union, Callable, List
import pyodbc

from confluent.data_types import CSVData,QueryData

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

    def __process_matched_hashes(self,bucket_row,probe_row, bucket_join_column_indexes, probe_join_column_indexes):
        return tuple(bucket_row),tuple(probe_row,)

    data_type = Union[CSVData,QueryData]
    def inner_join(self, input_1: data_type, input_2: data_type, override_build_join_key=None, override_process_matched_hashes=None):
        """Join two datasets.

        The smaller dataset should be passed into input_1.
        """

        if override_build_join_key != None:
            self.__build_join_key = override_build_join_key
        if override_process_matched_hashes != None:
            self.__process_matched_hashes = override_process_matched_hashes

        self.hash_buckets = defaultdict(list)

        for row in input_1.nextrow():
            self.__build_hash_input(row,input_1.join_column_indexes)

        for row in input_2.nextrow():
            join_key = self.__build_join_key(row,input_2.join_column_indexes)
            for bucket_row in self.hash_buckets[join_key]:
                record = self.__process_matched_hashes(bucket_row,row, input_1.join_column_indexes, input_2.join_column_indexes)
                if record != None:
                    yield record
                
