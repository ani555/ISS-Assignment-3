from pyspark import SparkContext, SparkConf
import time

start_time = time.time()

sc = SparkContext.getOrCreate()
ratings = sc.textFile("hdfs:///user/simmhan/ml/full/ratings.csv").cache()
header = ratings.first()
ratings_data_rdd = ratings.filter(lambda line: line!=header);

# Take all the ratings from the ratings data sort them and enumerate with zipWithIndex and get the middle value using lookup
ratings_rdd = ratings_data_rdd.map(lambda line: float(line.split(',')[2])).sortBy(lambda x: x).zipWithIndex().map(lambda x: (x[1],x[0]))
median_rating = ratings_rdd.lookup(ratings_rdd.count()/2)[0]
print 'Median value of ratings given by users :' + str(median_rating)

print 'Execution time : ' + str(time.time() - start_time)