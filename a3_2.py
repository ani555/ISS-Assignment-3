from pyspark import SparkContext, SparkConf
import time

start_time = time.time()

sc = SparkContext.getOrCreate()
ratings = sc.textFile("hdfs:///user/simmhan/ml/full/ratings.csv").cache()
header = ratings.first()

# ratings_data_rdd contains the ratings data without the header
ratings_data_rdd = ratings.filter(lambda line: line!=header);
ratings_data_rdd.cache()

# get the total ratings
total_ratings = ratings_data_rdd.count()

# raters_cnct contains the number of distinct users who rated a movie
raters_cnt = ratings_data_rdd.map(lambda line: line.split(',')[0]).distinct().count()
avg_num_ratings = float(total_ratings)/raters_cnt
print 'Average number of ratings given by users : ' + str(avg_num_ratings)

# ratings_sum contains the sum of all ratings given by users
ratings_sum = ratings_data_rdd.map(lambda line: float(line.split(',')[2])).sum()
avg_val_ratings = float(ratings_sum)/total_ratings
print 'Average value of ratings given by users : ' + str(avg_val_ratings)

print 'Execution time : ' + str(time.time() - start_time)