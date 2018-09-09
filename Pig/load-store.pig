-- Load data, which is comman-separated file
data = LOAD 'datasets/pigdata/data-bag.txt' USING PigStorage(',');
-- show result
hadoop fs -cat datasets/pigdata/data-bag.txt
-- output
1,2,3
1,2,3
1,2,4
2,3,4
3,4,5
4,5,6
4,5,6


/* Store data, this produces a directory with the name 'data-bag-piped' in which
there remains a file named part-m-00000 which contains the stored data */ 
STORE data INTO 'data-bag-piped' USING PigStorage('|'); 
-- show result 
hadoop fs -cat data-bag-piped/part-m-00000
-- output
1|2|3
1|2|3
1|2|4
2|3|4
3|4|5
4|5|6
4|5|6

