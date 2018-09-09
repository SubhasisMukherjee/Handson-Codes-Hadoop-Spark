-- Load data
data = LOAD 'datasets/pigdata/tweets.csv' USING PigStorage(',');

-- cut 6th and 8th fields using pig stream and cut unix command
filtered = STREAM data THROUGH `cut -f 6,8`;

