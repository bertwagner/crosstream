# crosstream 🐍🌊🤝

This Python package helps join large datasets across servers efficiently. It accomplishes this by streaming the data, allowing it to:
 - read each dataset only once
 - not need to store the complete datasets in memory

This is particularly helpful when your datasets are larger than the available memory on your machine or when you want to minimize network reads.

This package supports CSV and pyodbc datasources.

## Installation

From PyPI:
```
pip install crosstream
```

From source:

```
git clone https://github.com/bertwagner/crosstream.git
cd crosstream
pip install .
```

## Usage Examples

### Basic hash join

Ideally you want to make your smaller dataset the first dataset in your join.

```
import crosstream as cs
import csv

file1 = 'small_dataset.csv'
file2 = 'large_dataset.csv'

# join using column indexes or column names
c1 = cs.read_csv(file1,True,[0,1])
c2 = cs.read_csv(file2, True, ['col1','col2'])

# specify the output file
with open('joined_output.csv', 'w') as f:
    w = csv.writer(f)
    
    # write header column names
    w.writerow(c1.column_names + c2.column_names)

    for row_left,row_right in cs.inner_hash_join(c1,c2):
        # write matched results to our joined_output.csv
        w.writerow(row_left + row_right)
```

More examples can be found under the `tests` directory.

### Custom method for determining equality

By default, this package will join only if all values are equal.

If you want to perform a transformation on your data before comparing for equality, or use more complicated join equality logic, you can pass in your own function into `override_build_join_key` to define how equality is determined:

```
# define a function for transforming join key data before it's hashed
def custom_join_key_transform(value):
    transformed = value.replace(' ','')
    return transformed
```

And then pass that into the `inner_hash_join()` method:

```
...
for row_left,row_right in cs.inner_hash_join(c1,c2,override_join_key_transform=custom_join_key_transform)
...
```

### Custom method for processing matched hashes

By default this package returns tuples of the joined rows. If you want to customize what the output of your matched data looks like (perform transformations after a match is found), you can pass a function to the `override_process_matched_hashes` argument:

```
# define a function for performing additional transformations or adding additional outputs before the columns are returned
def custom_process_matched_hashes(bucket_row,probe_row, bucket_join_column_indexes, probe_join_column_indexes):
    # adding a new column indicating the weights of these matches are equal to 1
    weight=1.0
    return tuple(bucket_row),tuple(probe_row),(weight,)
```

And then pass that into the `inner_hash_join()` method:

```
for row_left,row_right,weight in cs.inner_hash_join(c1,c2,override_process_matched_hashes=custom_process_matched_hashes):
```


## Tests

If you want to run the tests, you will need to ensure you have the sqlite odbc driver installed:

```
apt-get install libsqliteodbc unixodbc
```

Then execute the tests:

```
pytest
```

And check for coverage:

```
pytest --cov-config=tests/.coveragerc --cov=crosstream --cov-report term-missing
```
