from pyspark import SparkContext, SparkConf
import time

start_time = time.time()

sc = SparkContext.getOrCreate()
movies = sc.textFile("hdfs:///user/simmhan/ml/full/movies.csv").cache()

header = movies.first()

# genres_rdd contains the (genre, cnt) pairs for each genre where cnt is the number of movies in which a genre occurs
genres_rdd = movies.filter(lambda line: line!=header).flatMap(lambda line: line.split(',')[-1].split('|')).map(lambda x: (x,1)).reduceByKey(lambda a,b: a+b).cache()

# flip the tuple and get the max and min
genre_max_movies = genres_rdd.map(lambda x: (x[1],x[0])).max()[1]
genre_min_movies = genres_rdd.map(lambda x: (x[1],x[0])).min()[1]
print 'Genres with most number of movies : ' + genre_max_movies
print 'Genres with least number of movies : ' + genre_min_movies

print 'Execution time : ' + str(time.time() - start_time)
