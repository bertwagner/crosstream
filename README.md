# qross

A package for streaming dataset joins across servers while using minimal memory. `qross` is able to join:
 - CSV to CSV
 - CSV to ODBC 
 - ODBC to ODBC

 `qross` is optimized to use minimal memory by streaming the data. This allows even the most low-performance machines to handle large dataset joins that would otherwise not be able to fit in the machine's memory.

## Installation

```
git clone https://github.com/bertwagner/python_streaming_data_joiner.git
cd python_streaming_data_joiner
pip install .
```

## Basics

Ideally you want to make your smaller dataset the first dataset in your join.


```
from data_joiner.hash_join import HashJoin
from data_joiner.data_types import CSVData,QueryData
import csv

file1 = 'small_dataset.csv'
file2 = 'large_dataset.csv'

# join using column indexes or column names
c1=CSVData(file1,True,[0,1])
c2=CSVData(file2, True, ['col1','col2'])

# initialize the type of join to perform. 
h=HashJoin()

with open('joined_output.csv', 'w') as f:
    w =csv.writer(f)
    
    # write header column names
    w.writerow(c1.column_names + c2.column_names)

    for row_left,row_right in h.inner_join(c1,c2):
        # write matched results to our joined_output.csv
        w.writerow(row_left + row_right)
```

 ## Loop versus Hash Joins

 

 ## Custom methods for determining equality