from collections import defaultdict
from typing import overload, Union, Callable, List

from crosstream.readers import CSVReader,ODBCReader

class HashJoin:
    def __init__(self):
        self.hash_buckets = defaultdict(dict)
        self.hash_buckets_key_values = defaultdict(list)

    def __build_hash_input(self, row, join_column_indexes):
        join_key, join_key_values = self.__build_join_key(row,join_column_indexes)
        self.hash_buckets[join_key].append(row)
        self.hash_buckets_key_values[join_key].append(join_key_values)

    def __build_join_key(self,row,indices):
        # calculate the hash of join values
        join_values = []
        join_key_values = []
        for col_index in indices:
            # convert row[col_index] to string because CSVs can't don't define numeric datatypes - would have to interpret them.
            col_value = str(row[col_index])
            join_values.append(str(hash(col_value)))
            join_key_values.append(col_value)
        join_key = ''.join(join_values)

        return join_key, join_key_values

    def __process_matched_hashes(self,bucket_row,probe_row, bucket_join_column_indexes, probe_join_column_indexes):
        return tuple(bucket_row),tuple(probe_row,)

    reader = Union[CSVReader,ODBCReader]
    def inner_join(self, input_1: reader, input_2: reader, override_build_join_key=None, override_process_matched_hashes=None):

        if override_build_join_key != None:
            self.__build_join_key = override_build_join_key
        if override_process_matched_hashes != None:
            self.__process_matched_hashes = override_process_matched_hashes

        self.hash_buckets = defaultdict(list)

        for row in input_1:
            self.__build_hash_input(row,input_1.join_column_indexes)

        for row in input_2:
            join_key, join_key_values = self.__build_join_key(row,input_2.join_column_indexes)
            for bucket_row in self.hash_buckets[join_key]:
                # verify join keys match and not just a hash collision
                input_1_join_keys = self.hash_buckets_key_values[join_key][0]
                input_2_join_keys = join_key_values
                if input_1_join_keys == input_2_join_keys:
                    record = self.__process_matched_hashes(bucket_row,row, input_1.join_column_indexes, input_2.join_column_indexes)
                    if record != None:
                        yield record