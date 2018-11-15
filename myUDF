-- create table
CREATE table mytable(
fname STRING,
lname STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ‘\t’ 
STORED AS TEXTFILE;

-- select data
SELECT * FROM mytable;
--output
RAVI    KUMAR
Anish   Kumar
Rakesh  JHA
vishal  kumar
Ananya  GHOSH

-- load data
LOAD DATA LOCAL INPATH 'names.csv' OVERWRITE INTO TABLE mytable;

-- add the Python script into Hive’s classpath
add FILE 'myudf.py'

-- call the udf which converts all the lname column to lowercase
SELECT TRANSFORM(fname, lname) USING ‘python my.py’ AS (fname, l_name) FROM mytable;
--output
RAVI    kumar
Anish   kumar
Rakesh  jha
vishal  kumar
Ananya  ghosh
