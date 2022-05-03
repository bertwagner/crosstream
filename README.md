# Python Streaming Data Joiner

A cross server data joiner, written to handle large data sizes without running out of memory.

```
import csv

c1=CSVData(file1,True,[0,1])
c2=CSVData(file2, True, ['col1','col2'])

h=HashJoin()


with open('joined_data.csv', 'w') as f:
    w =csv.writer(f)
    
    # write header column names
    w.writerow(c1.column_names + c2.column_names)

    for row_left,row_right in h.inner_join(c1,c2):
        # write matched results
        w.writerow(row_left + row_right)
```