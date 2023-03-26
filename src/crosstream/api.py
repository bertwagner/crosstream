from typing import overload, Union, Callable, List
from crosstream.readers import CSVReader, ODBCReader
from crosstream.hash_join import HashJoin

@overload
def read_csv(csv_file_path: str, has_headers: bool, join_columns: List[str], encoding: str):
    ...

@overload
def read_csv(csv_file_path: str, has_headers: bool, join_columns: List[int], encoding: str):
    ...

def read_csv(csv_file_path, has_headers, join_columns, encoding='utf-8'):
    """Lazily load the contents of a csv file one row at a time as needed."""

    return CSVReader(csv_file_path,has_headers,join_columns,encoding)


@overload
def read_odbc(connection_string: str, query: str, join_columns: List[str]):
    ...

@overload
def read_odbc(connection_string: str, query: str, join_columns: List[int]):
    ...

def read_odbc(connection_string, query, join_columns):
    """Lazily load the contents of an ODBC sql query one row at a time as needed."""
    return ODBCReader(connection_string, query, join_columns)

reader = Union[CSVReader,ODBCReader]
def inner_hash_join(input_1: reader, input_2: reader, override_build_join_key=None, override_process_matched_hashes=None):
    """Join two datasets using a hash join.

        The smaller dataset should be passed into input_1.
        """
    h=HashJoin()
    return h.inner_join(input_1,input_2,override_build_join_key,override_process_matched_hashes)