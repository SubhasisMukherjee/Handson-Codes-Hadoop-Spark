-- Register python UDF
REGISTER 'myudf.py' USING jython AS my_sample_udf
-- output 
2018-09-08 20:55:28,904 [main] INFO  org.apache.pig.scripting.jython.JythonScriptEngine - created tmp python.cachedir=/tmp/pig_jython_8366976098497652886
2018-09-08 20:55:31,705 [main] WARN  org.apache.pig.scripting.jython.JythonScriptEngine - pig.cmd.args.remainders is empty. This is not expected unless on testing.
2018-09-08 20:55:31,713 [main] INFO  org.apache.pig.scripting.jython.JythonScriptEngine - Register scripting UDF: my_sample_udf.hello_world
2018-09-08 20:55:31,714 [main] INFO  org.apache.pig.scripting.jython.JythonScriptEngine - Register scripting UDF: my_sample_udf.findvim
2018-09-08 20:55:31,714 [main] INFO  org.apache.pig.scripting.jython.JythonScriptEngine - Register scripting UDF: my_sample_udf.increase_age

-- Load data
tweet_data = LOAD 'datasets/pigdata/tweets.csv' USING PigStorage(',');

-- Eliminate double quotes from source and tweet fields
clean_tweet_data = FOREACH tweet_data GENERATE $0,$1,$2,$3,$4,$5,REPLACE($6,'"',''),REPLACE($7,'"','');

-- Filter tweets made from twitvim
twitvim_tweets = FILTER clean_tweet_data BY my_sample_udf.findvim($6) == TRUE;

-- Extract tweet source and texts
tweets = FOREACH twitvim_tweets GENERATE $7;

-- Show results
DUMP tweets;
