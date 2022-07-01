from pyspark import SparkContext, SparkConf
import time

start_time = time.time()


sc = SparkContext.getOrCreate()
movies = sc.textFile("hdfs:///user/simmhan/ml/full/movies.csv").cache()


# Given a line this function takes the movieId - tokens[0] and genres - tokens[-1] creates (movieId,genre) tuples for that movieId and returns as a list
# eg: for movieId '1' and genres 'Comedy|Drama' it returns [(1,'Comedy'),(1,'Drama')]
def zip_mvid_genre(line):
	tokens = line.split(',')
	genres_list = tokens[-1].split('|')
	mvid_list = [tokens[0]]*len(genres_list)
	return zip(mvid_list,genres_list)



header = movies.first()

# mvid_genre_rdd contains the (mvid, genre) pairs for all movies , eg [(1, 'Drama'), (1,'Comedy') (2, 'Action') ....]
mvid_genre_rdd = movies.filter(lambda line: line!=header).flatMap(zip_mvid_genre).cache()

# genrepair_cnt_kvp contains the count corresponding each genre pair eg : [('Comedy|Drama', 10), ('Action|Drama', 3) ....]
# As (genre1, genre2) and (genre2, genre1) are one and the same while forming the key they are ordered lexicographically before forming the key
genrepair_cnt_kvp = mvid_genre_rdd.join(mvid_genre_rdd).filter(lambda x: x[1][0]!=x[1][1]).map(lambda x: (x[1][0]+'|'+x[1][1], 1) if x[1][0]<x[1][1] else (x[1][1]+'|'+x[1][0] , 1)).reduceByKey(lambda a,b: a+b).cache()

# flip the tuples and get the genre pair corresponding max and the min counts
genrepair_max = genrepair_cnt_kvp.map(lambda x: (x[1],x[0])).max()[1]
genrepair_min = genrepair_cnt_kvp.map(lambda x: (x[1],x[0])).min()[1]
print 'Most likely pair of genres to occur together : ' + genrepair_max
print 'Least likely pair of genres to occur together : ' + genrepair_min

print 'Execution time : ' + str(time.time() - start_time)