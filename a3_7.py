from pyspark import SparkContext, SparkConf
import math
import time

start_time = time.time()


sc = SparkContext.getOrCreate()
movies = sc.textFile("hdfs:///user/simmhan/ml/full/movies.csv").cache()
genome_scores = sc.textFile("hdfs:///user/simmhan/ml/full/genome-scores.csv").cache()


# this function takes a line from the movies data and replaces ',' if any in the movie title with '-'
def process(line):
	start = line.find('"')
	end = line.rfind('"')
	if start==-1:
		return line
	parts = line.partition(line[start:end])
	return parts[0]+line[start:end].replace(',','-')+parts[-1]
	
# set the source movie id
src_mvid = '1'
header = genome_scores.first()

# mvid_relvec_rdd contains tuples of the form (mvId, rel_list), The rel_list contains the relevance scores ordered on the basis of tags to ensure uniformity across movies
mvid_relvec_rdd = genome_scores.filter(lambda line: line!=header).map(lambda line: line.split(',')).map(lambda x: (x[0],(x[1],x[2]))).groupByKey().map(lambda x: (x[0],[float(rel) for tag, rel in sorted(x[1])])).cache()

# get the source vector
src_vector = mvid_relvec_rdd.lookup(src_mvid)[0]

header = movies.first()
# movie_names_rdd contains the tuples of the form (mvId, title)
# Each line is first processed to ensure that movie titles containing ',' are replaced with '-' 
movie_names_rdd = movies.filter(lambda line: line!=header).map(process).map(lambda line: line.split(',')[0:2]).map(lambda x: (x[0],x[1])).cache()

# Calculate the distance of each movie from the source movie , join the result with movie_names_rdd, filter out the source movie and take the top 10 ordered in ascending order of distance
top_10 = mvid_relvec_rdd.map(lambda x: (x[0], math.sqrt(sum([(src_vector[i] - x[1][i])**2 for i in range(len(x[1]))])))).join(movie_names_rdd).filter(lambda x: x[0]!=src_mvid).takeOrdered(10, key= lambda x: x[1][0])

print 'Source Movie: '+ src_mvid + ' ' + movie_names_rdd.lookup(src_mvid)[0]
print 'Top 10 movies whose genome scores have nearest euclidean distance to the source movie:'

for movie_id, (rel, movie_name) in top_10:
	print 'Id: ' + movie_id + ' Name: ' + movie_name

print 'Execution time : ' + str(time.time() - start_time)
