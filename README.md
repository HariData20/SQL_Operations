# SQL_Operations
Performs Range and Round Robin Partitions, Parallel Sort and Join

## Partition and Insert Functions
### Range_Partition()  
(1) the Ratings table stored in PostgreSQL and 
(2) an integer value N; that represents the number of
partitions. Range_Partition() then generates N horizontal fragments of the Ratings table
and store them in PostgreSQL. The algorithm partitions the ratings table based on
N uniform ranges of the Rating attribute.
### RoundRobin_Partition():
(1) the Ratings table stored in PostgreSQL and
(2) an integer value N; that represents the number of
partitions. The function then generates N horizontal fragments of the Ratings table and
stores them in PostgreSQL. The algorithm partitions the ratings table using the
round robin partitioning approach.
### RoundRobin_Insert() 
that takes input: (1) Ratings table
stored in PostgreSQL, (2) UserID, (3) MovieID, (4) Rating. RoundRobin_Insert() then
inserts a new tuple to the Ratings table and the right fragment based on the round robin
approach.
### Range_Insert() 
that takes input: (1) Ratings table stored in Postgresql (2) UserID, (3) MovieID, (4) Rating. 
### Range_Insert() then inserts a new
tuple to the Ratings table and the correct fragment (of the partitioned ratings table)
based upon the Rating value.

## Query Processing
### RangeQuery() -
Takes input: (1) Ratings table stored in
PostgreSQL, (2) RatingMinValue, (3) RatingMaxValue, and (4) openconnection
RangeQuery would not use ratings table but it would use the range and round robin partitions of the ratings table.
RangeQuery() returns all tuples for which the rating value is larger than or equal to RatingMinValue and less than or equal to RatingMaxValue.
 The returned tuples arestored in a text file, named RangeQueryOut.txt Each line of the file represents a
tuple that has the following format:
○ Example:
PartitionName, UserID, MovieID, Rating
RangeRatingsPart0,1,377,0.5
RoundRobinRatingsPart1,1,377,0.5
In these examples, PartitionName represents the full name of the partition (such
as RangeRatingsPart1 or RoundRobinRatingsPart4) in which the output tuple
resides.
PartitionName, UserID, MovieID, and Rating.
### PointQuery() -
Takes ainput: (1) Ratings table stored in
PostgreSQL, (2) RatingValue, and (3) openconnection
PointQuery would not use ratings table but it would use the range and round robin partitions of the ratings table.
PointQuery() returns all tuples for which the rating value is equal to RatingValue.
RatingValue
● The returned tuples are stored in a text file, named PointQueryOut.txt such that each line represents a tuple that has
the following format such that PartitionName represents the full name of the partition (i.e.
RangeRatingsPart1 or RoundRobinRatingsPart4, etc.) in which this tuple resides.
○ Example:
PartitionName, UserID, MovieID, Rating
RangeRatingsPart3,23,459,3.5
RoundRobinRatingsPart4,31,221,0

## Parallel Sort and Join
### ParallelSort() 
that takes input: (1) InputTable stored in a
PostgreSQL database, (2) SortingColumnName the name of the column used to order
the tuples by. ParallelSort() then sorts all tuples (using five parallelized threads) and
stores the sorted tuples in a table named OutputTable (the output table name is passed
to the function). The OutputTable contains all the tuple present in InputTable sorted in
ascending order.
### ParallelJoin() 
that takes input: (1) InputTable1 and
InputTable2 table stored in a PostgreSQL database, (2) Table1JoinColumn and
Table2JoinColumn that represent the join key in each input table respectively.
ParallelJoin() then joins both InputTable1 and InputTable2 (using five parallelized
threads) and stored the resulting joined tuples in a table named OutputTable (the output
table name is passed to the function). The schema of OutputTable will be similar to
the schema of both InputTable1 and InputTable2 combined.
