from pyspark import SparkContext, SparkConf
import time

start_time = time.time()

sc = SparkContext.getOrCreate()
ratings = sc.textFile("hdfs:///user/simmhan/ml/full/ratings.csv").cache()

header = ratings.first()

# raters_rdd contains all the distinct user ids  who have rated a movie
raters_rdd = ratings.filter(lambda line: line!=header).map(lambda line: line.split(',')[0]).distinct().cache()
print 'Number of distinct users who have rated movies : ' + str(raters_rdd.count())

tags = sc.textFile("hdfs:///user/simmhan/ml/full/tags.csv").cache()
header = tags.first()

# taggers_rdd contains all the distinct user ids who have tagged a movie
taggers_rdd = tags.filter(lambda line: line!=header).map(lambda line: line.split(',')[0]).distinct().cache()
print 'Number of distict users who have tagged movies : ' + str(taggers_rdd.count())

# Number of users who have both rated and tagged a movie
raters_taggers_cnt = raters_rdd.intersection(taggers_rdd).count()
print 'Number of users who have both given ratings and tagged movies : ' + str(raters_taggers_cnt)

print 'Execution time : ' + str(time.time() - start_time)