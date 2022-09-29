# crosstream 🐍🌊🤝

This Python package helps join large datasets across servers efficiently. It accomplishes this by streaming the data, allowing it to:
 - read each dataset only once
 - not need to store the complete datasets in memory

This is particularly helpful when your datasets are larger than the available memory on your machine or when you want to minimize network reads.

This package supports CSV and pyodbc datasources.

## Installation

```
git clone https://github.com/bertwagner/crosstream.git
cd crosstream
pip install .
```

## Usage Examples

### Basic hash join

Ideally you want to make your smaller dataset the first dataset in your join.

```
from crosstream.hash_join import HashJoin
from crosstream.data_types import CSVData,QueryData
import csv

file1 = 'small_dataset.csv'
file2 = 'large_dataset.csv'

# join using column indexes or column names
c1=CSVData(file1,True,[0,1])
c2=CSVData(file2, True, ['col1','col2'])

# initialize the type of join to perform. 
h=HashJoin()

# specify the output file
with open('joined_output.csv', 'w') as f:
    w =csv.writer(f)
    
    # write header column names
    w.writerow(c1.column_names + c2.column_names)

    for row_left,row_right in h.inner_join(c1,c2):
        # write matched results to our joined_output.csv
        w.writerow(row_left + row_right)
```
### Custom method for determining equality

By default, this package will join only if all values are equal.

If you want to perform a transformation on your data before comparing for equality, or use more complicated join equality logic, you can pass in your own function into `override_build_join_key` to define how equality is determined:


### Custom method for processing matched hashes

By default this package returns tuples of the joined rows. If you want to customize what the output of your matched data looks like, you can pass a function to the `override_process_matched_hashes` argument:


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
pytest --cov-config=tests/.coveragerc --cov=confluent --cov-report term-missing
```
