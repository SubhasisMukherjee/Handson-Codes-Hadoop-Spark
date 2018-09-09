hadoop fs -cat datasets/pigdata/multi-delimiter.txt
key1    value11,value12,value13
key2    value21,value22,value23
key3    value31,value32,value33
key4    value41,value42,value43

-- Load data
outerbag = LOAD 'datasets/pigdata/multi-delimiter.txt' USING PigStorage('\t') AS (f1, f2);
DUMP outerbag;
-- output
(key1,value11,value12,value13)
(key2,value21,value22,value23)
(key3,value31,value32,value33)
(key4,value41,value42,value43)

innerbag = FOREACH outerbag GENERATE f1, STRSPLIT(f2, ',');
DUMP innerbag;
--out put
(key1,(value11,value12,value13))
(key2,(value21,value22,value23))
(key3,(value31,value32,value33))
(key4,(value41,value42,value43))
