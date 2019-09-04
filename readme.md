# Redis Search Demo

This demo show redis search

## Sales
This demo contains data about sales of toy automobiles

### To load the data
```
cd Search/sales
./create.sh localhost 6379 sales
```
The create script receives 3 parameters: redis host, redis port and index name

### To run the queries
```
python ../SearchDemo.py -s localhost -p 6379 -i sales -d queries.txt
```

Make sure that the index name provided with the '-i' option, is the same as created above.

The SearchDemo python script runs all the queries in the queries.txt file.
If you do not specify the -d parameter with the queries file, it runs in interactive mode and allows you to run any search query on the given index.

## NY Times
This demo contains data about articles published in the New York Times and show text search.

### To load the data
```
cd Search/nytimes
./create.sh localhost 6379 nytimes
```
The create script receives 3 parameters: redis host, redis port and index name

### To run the queries
```
python ../SearchDemo.py -s localhost -p 6379 -i nytimes -d queries.txt
```

Make sure that the index name provided with the '-i' option, is the same as created above.

The SearchDemo python script runs all the quries in the queries.txt file.
If you do not specify the -d parameter with the queries file, it run in interactive mode and allows you to run any search query on the given index.
