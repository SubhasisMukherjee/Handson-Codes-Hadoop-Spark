/* Place the UDF file myudf.py in local path and register to pig using jython
At this point, myudf.py contains only hello_world() function */
REGISTER 'myudf.py' USING jython AS my_sample_udf
--output
2018-09-08 15:35:42,947 [main] INFO  org.apache.pig.scripting.jython.JythonScriptEngine - created tmp python.cachedir=/tmp/pig_jython_2005350041424529700
2018-09-08 15:35:45,859 [main] WARN  org.apache.pig.scripting.jython.JythonScriptEngine - pig.cmd.args.remainders is empty. This is not expected unless on testing.
2018-09-08 15:35:45,867 [main] INFO  org.apache.pig.scripting.jython.JythonScriptEngine - Register scripting UDF: my_sample_udf.hello_world

-- Load user data
users = LOAD 'user_data.csv' USING PigStorage(',') AS (fname:chararray,lname:chararray,age:int);

-- show data
DUMP users;
--output
(subhasis,mukherjee,33)
(debapriya,mukherjee,32)
(mrinmoyee,mookherjee,65)

-- Calling udf and create new relation
hello_data = FOREACH users GENERATE CONCAT(my_sample_udf.hello_world(),' from ',fname,' ',lname);

-- show data
DUMP hello_data;
--output
(Hello World from subhasis mukherjee)
(Hello World from debapriya mukherjee)
(Hello World from mrinmoyee mookherjee)

/* After this we change the myudf.py to include increase_age() function and re-register 
Then we try to call the new function from pig */
REGISTER 'myudf.py' USING jython AS my_sample_udf
new_age = FOREACH users GENERATE my_sample_udf.increase_age();
-- output
2018-09-08 15:55:39,959 [main] ERROR org.apache.pig.tools.grunt.Grunt - ERROR 1070: Could not resolve my_sample_udf.increase_age using imports: [, java.lang., org.apache.pig.builtin., org.apache.pig.impl.builtin.]

/* It seems the changed udf does not take effect when registered with the same alias name, hence register with new alias name */
REGISTER 'myudf.py' USING jython AS my_sample_udf2
--output
2018-09-08 16:02:18,134 [main] WARN  org.apache.pig.scripting.jython.JythonScriptEngine - pig.cmd.args.remainders is empty. This is not expected unless on testing.
2018-09-08 16:02:18,138 [main] INFO  org.apache.pig.scripting.jython.JythonScriptEngine - Register scripting UDF: my_sample_udf2.hello_world
2018-09-08 16:02:18,138 [main] INFO  org.apache.pig.scripting.jython.JythonScriptEngine - Register scripting UDF: my_sample_udf2.increase_age

-- Call new udf
new_age = FOREACH users GENERATE fname,my_sample_udf2.increase_age(age);

-- Show data
DUMP new_age;
-- output
(subhasis,35)
(debapriya,34)
(mrinmoyee,67)
  
 



