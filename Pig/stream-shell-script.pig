-- Load data
data = LOAD 'datasets/pigdata/tweets.csv' USING PigStorage(',');

-- cut 6th and 8th fields using pig stream and cut unix command
filtered = STREAM data THROUGH `cut -f 6,8`;
DUMP filtered;
--output
("timestamp","text")
("2013-04-01 08:44:20 +0000","@ICICIBank_Care hope you guys fix it. It is really very irritating.")
("2013-04-01 12:42:01 +0000","Finally)
("2013-04-01 12:51:13 +0000","@realsubbuj there is a decent rain in koramangala.")
("2013-04-04 03:24:24 +0000","Creating new pages in #WordPress automatically through code http://t.co/bkTX2m8104")
("2013-04-05 06:01:13 +0000","Today is ""bring your kids"" day at office and the entire office is taken over by cute little creatures ;)")
("2013-04-06 12:14:31 +0000","@Bill_Porter All posts from your website http://t.co/NUWn5HUFsK seems to have been deleted. I am getting a ""Not Found"" page even in homepage")
("2013-04-08 07:01:02 +0000","@neetashankar Yeah)
("2013-04-08 15:11:46 +0000","@sudhamshu after trying out various tools to take notes and I found that paper is the best to take notes and to maintain todo lists.")
("2013-04-11 03:11:31 +0000","@Bill_Porter nice to know that your site is back :-)")

