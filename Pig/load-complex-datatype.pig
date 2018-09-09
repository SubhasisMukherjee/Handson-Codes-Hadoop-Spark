-- load data from file which has complex data 
data = LOAD 'datasets/pigdata/nested-schema.txt' AS (f1:int, f2:bag{t:tuple(n1:int, n2:int)}, f3:map[]);

-- show data
DUMP data;
-- result
(1,{(1,2),(2,3)},[name#sudar,age#30])
(2,{(2,3),(4,5)},[name#muthu,age#30])
(3,{(3,4),(6,7)},[name#haris,age#22])
(4,{(4,5),(4,5)},[name#Dinesh,age#25])

-- load seperate columns and display
a = FOREACH data GENERATE f1;
DUMP a;
-- output
(1)
(2)
(3)
(4)

b = FOREACH data GENERATE f2[0],f2[1];
