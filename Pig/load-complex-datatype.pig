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
by_pos = FOREACH data GENERATE f1;
DUMP by_pos;
-- output
(1)
(2)
(3)
(4)

by_field = FOREACH data GENERATE f2;
DUMP by_field;
--output
({(1,2),(2,3)})
({(2,3),(4,5)})
({(3,4),(6,7)})
({(4,5),(4,5)})

x = FOREACH data GENERATE FLATTEN(f2);
DUMP x;
-- output
(1,2)
(2,3)
(2,3)
(4,5)
(3,4)
(6,7)
(4,5)
(4,5)

by_map = FOREACH data GENERATE f3#'name';
DUMP by_map;
-- output 
(sudar)
(muthu)
(haris)
(Dinesh)

