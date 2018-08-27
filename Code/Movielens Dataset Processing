# Import necessary modules
from pyspark.sql.functions import *

# Define dataset location
home_dir = "/user/subhtech099501/Movielens/"

# Create a dataframe on movies.csv file
movies = spark.read.format("csv") \
.options(header = True) \
.options(inferSchema = True) \
.load(home_dir + "movies.csv") \
.cache() # Keep the dataframe in memory for faster processing

# Print dataframe schema
movies.printSchema()
# List all column names and their datatypes, in tuple format
movies.dtypes
# Returns the schema of this DataFrame as a pyspark.sql.types.StructType
movies.schema
# Display datafrmae 5 rows
movies.show(5)
# Find the number of records in movies dataframe
movies.count()

# Create ratings Dataframe using ratings.csv file
ratings = spark.read.format("csv") \
.options(header = True) \
.options(inferSchema = True) \
.load(home_dir + "ratings.csv") \
.persist()

# Print schema, column, datatypes, count and display rows
ratings.printSchema()
ratings.dtypes
ratings.show(5)
ratings.count()  --100004

# Group rating dataframe by movie-user, count such groups
ratings.groupBy("movieId", "userId").count().show()
+-------+------+-----+
|movieId|userId|count|
+-------+------+-----+
|    858|     4|    1|
|   1307|     4|    1|
|   2716|     4|    1|
|   2502|     6|    1|
|   4310|    13|    1|
|    919|    15|    1|
|   2541|    15|    1|
|   2726|    15|    1|
|   7438|    15|    1|
|   8910|    15|    1|
|  58559|    15|    1|
| 135436|    15|    1|
| 136864|    15|    1|
|   2021|    17|    1|
|   2067|    17|    1|
|    456|    19|    1|
|    501|    19|    1|
|   1336|    19|    1|
|    527|    20|    1|
|    380|    21|    1|
+-------+------+-----+

# Filter those user-movie combo which is has multiple entries (erroneous records)
ratings.groupBy(['movieId','userId']).count().filter("count != 1").show()
+-------+------+-----+
|movieId|userId|count|
+-------+------+-----+
+-------+------+-----+

# As there is no duplicate, number of such combos should be same as total entries in rating dataset
ratings.groupBy(["movieId", "userId"]).count().count() --100004

# Count number of reviews for each movie
ratings.groupBy('movieId') \
.count() \
.withColumnRenamed('count','ratingcount') \
.show()

# Filter those movies which have atleast 100 ratings
ratings.groupBy('movieId') \
.count() \
.withColumnRenamed('count','ratingcount') \
.filter("ratingcount >= 100").show()

# Find movies rating count and average ratings and filter movies with atleast 100 ratings
ratings.groupBy("movieId") \
.agg(count("movieId").alias("rating_count") \
    ,avg("rating").alias("avg_rating")) \
.filter('rating_count >= 100') \
.show()

# Order the above result by avg ratings desc
ratings.groupBy("movieId") \
.agg(count("movieId").alias("rating_count") \
    ,avg("rating").alias("avg_rating")) \
.filter('rating_count >= 100') \
.orderBy(desc("avg_rating")) \
.show()

# ANOTHER WAY FOR ABOVE
ratings_agg = ratings.groupBy("movieId") \
.agg(count("movieId").alias("count"), \
     avg("rating").alias("avg_rating"))
ratings_agg.show()

ratings_agg.alias("t1") \
.join(movies.alias("t2"), col("t1.movieId") == col("t2.movieId")) \
.filter("count > 100") \
.orderBy(desc("avg_rating")) \
.select("t1.movieId", "title", "avg_rating", "count") \
.limit(10) \
.show()


# Show temporary views for current Spark session
sql("use subhtech09");
sql("show tables").show()

movies.createOrReplaceTempView("movies")
ratings.createOrReplaceTempView("ratings")

# Using SQL statement, find top 10 movies based on the highest average ratings. 
# Consider only those movies that have at least 100 ratings. 
# Show movieId, title, average rating and rating count columns.
sql("select r.movieId, m.title, avg(r.rating) as avg_rating,count(r.movieId) as rating_count \
from ratings r join movies m on r.movieId = m.movieId \
group by r.movieId,m.title \
having count(r.movieId) >= 100 \
order by avg_rating desc \
limit 10").show()

###############################
# EXPLODE and SPLIT --> usage
###############################

df = spark.createDataFrame([Row(a=1, intlist=[1,2,3], mapfield={"a": "b"})])
df.show()
+---+---------+--------+
|  a|  intlist|mapfield|
+---+---------+--------+
|  1|[1, 2, 3]|[a -> b]|
+---+---------+--------+
df.select(explode(df.intlist).alias("exploded")).show()
+--------+
|exploded|
+--------+
|       1|
|       2|
|       3|
+--------+

df2 = spark.createDataFrame([Row(a=1, intlist=["1,2,3"], mapfield={"a": "b"})])
df2.show()
+---+-------+--------+
|  a|intlist|mapfield|
+---+-------+--------+
|  1|[1,2,3]|[a -> b]|
+---+-------+--------+
df2.select(explode(df2.intlist).alias("exploded")).show()
+--------+
|exploded|
+--------+
|   1,2,3|
+--------+
** The above does not explode, as this is no longer a list of items 1,2,3 but it is now a string "1,2,3" inside a list element
** To handle this, we need to split then explode
** But if we apply split(df2.intlist), split will not work on a list. Hence we change the data here.

df2 = spark.createDataFrame([Row(a=1, intlist="1,2,3", mapfield={"a": "b"})])
df2.show()
df2.select(explode(split(df2.intlist,",")).alias("exploded")).show()
+--------+
|exploded|
+--------+
|       1|
|       2|
|       3|
+--------+
###################################################################33

#Find average rating of each genre (advanced)
ratings.alias("r") \
.join(movies.alias("m"), col("r.movieId") == col("m.movieId")) \
.select(ratings.rating,explode(split(movies.genres,"\|")).alias("genre")) \
.groupBy(col("genre")) \
.agg(count(col("genre")).alias("rating_count"), \
     avg(ratings.rating).alias("avg_rating")) \
.orderBy(desc("avg_rating")) \
.show()














