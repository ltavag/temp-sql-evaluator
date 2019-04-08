## SQL Evaluator

#### Usage:
* To run code: 
    ```python3 sql_evaluator.py examples examples/cities-1.sql.json examples/cities-1.out```
    
* To run tests: 
    ```./check python3 sql_evaluator.py -- examples --examples/cities-1.sql```

* Note: The error test cases do not currently pass since the error messages have been formatted differently. All of the cases have been tested to ensure that we are throwing a semantically correct error. 

#### Components
`table.py` - This file is a small class based wrapper around lists. I was mostly inspired by the Dict Cursor for pyscopg2 for dealing with rows, so I used a dictionary to implement each row. While it makes things very readable, It obviously has significant impacts on memory usage since we are storing the keys for each row. Given more time I might explore switching to a list based system here, along side some convenience functions that would allow you to treat the list as a dict.  

`table_filter.py` - This file defines a class that allows you to dynamically create filter functions. It is initialized from the conditions present in `query['where']`, and fullfills the callable interface that can be used alongside with `itertools.filter`.

`sql_evaluator.py` - This is the main file. It accounts for a majority of the validation logic and the execution of the queries. 

#### Optimizations

###### Where clause analysis: 
All where clauses are checked to see if they require multiple tables to be evaluated. If not we are able to evaluate certain conditions before we create the cross product for the Join. This greatly reduces the number of total rows that are scanned at the end of the join. 

###### Pipelining
All filters, joins are evaluated through generators. The implication of this is that we don't necessarily need to ever hold the full result set in memory. Since the result set is currently output as a JSON object, it is held in memory before serialization, although this could easily be changed by emitting a CSV. 
