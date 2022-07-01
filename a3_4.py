from pyspark import SparkContext, SparkConf
import time

start_time = time.time()

sc = SparkContext.getOrCreate()
movies = sc.textFile("hdfs:///user/simmhan/ml/full/movies.csv").cache()

header = movies.first()

# get the count of genres corresponding to each movie by calculating the length of list after splitting the genres
most_genres = movies.filter(lambda line: line!=header).map(lambda line: len(line.split(',')[-1].split('|'))).max() 
print 'Most number of genres assigned to any movie : ' + str(most_genres)

print 'Execution time : ' + str(time.time() - start_time)