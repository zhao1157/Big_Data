from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local[*]').setAppName('test_mapValues')
sc = SparkContext(conf = conf)

#======== 9 ======
#This is to practice cache(), which store the data in memory, and persist() which has more 
#options to store the data, but by default persist() is the same as cache()
x = sc.parallelize(range(10))

xx = x.map(lambda a: a**2)
xx.persist() #cache xx data, which is needed for future manipulation.

xxx = xx.filter(lambda a: a > 20)


xx.unpersist() #xx is no longer needed, so garbage collect it.

y = sc.parallelize([2,3])



"""


#======== 8 ========
#This is to practice accumulators
accum = sc.accumulator(0) #only int

x = sc.parallelize([2,3,4,5])

def f(a):
	global accum
	accum += a

x.foreach(f)
print('\n', accum.value, '\n')

def g(a):
	accum.add(-a)
	
x.foreach(g)
print ('\n', accum, '\n')



#======= 7 ========
#This is to practice broadcast
bdct_1 = sc.broadcast(2)
bdct_2 = sc.broadcast({'a': 8})
bdct_3 = sc.broadcast([2,7])

x = sc.parallelize(range(10))

print ('\n', 'Larger than bdct_1', x.filter(lambda a: a>bdct_1.value).collect(), '\n')
print ('\n', 'Smaller than bdct_2', x.filter(lambda a: a< bdct_2.value['a']).collect(), '\n')
print ('\n', 'Between bdct_3', x.filter(lambda a: a>bdct_3.value[0] and a< bdct_3.value[1]).collect(), '\n')
x.foreach(lambda x: print ('\n', x+bdct_2.value['a'], '\n'))



#======== 6 =========
#This is to practice two pair-RDD operations
x = sc.parallelize([(2,3), (2,4), (3,7), (4,7), (4,700)])
y = sc.parallelize([(2,'3'), (2,'4'), (4,70), (5,5)])

print ('\n', x.subtractByKey(y).collect(), '\n') #remove the sharing keys in y
x_y_cogroup = x.cogroup(y).collect() #a list of tuple(key, (values))
for key, value in x_y_cogroup:
	for v in value:
		print ('\n', key, list(v), '\n')

print ('\n', x.join(y).collect(), '\n')	#join() only for the sharing keys with all possible combinaiton of values
	
print ('\n', x.leftOuterJoin(y).collect(), '\n') #with keys in x as the base, if not exist in y, then y value as None

print ('\n', x.rightOuterJoin(y).collect(), '\n') #with keys in y as the base, if not exist in x, then x value as None


#======== 5 ========
#This is to practice pair RDD
x = sc.parallelize([(2,3,4), (2,3,5), (2,4,9)]) #when using pair RDD, only the first two columns are considered
											   #the first as the key and the second as the value
											  
print ('\n', x.keys().collect(),'\n')											  
print ('\n', x.values().collect(), '\n')
print ('\n', x.countByKey().items(), '\n')
#print ('\n', x.groupByKey().collect(), '\n') #does not work for more than one value for each key, nor does sortByKey() works.
print ('\n', list(x.map(lambda x: (x[0], tuple(x[1:]))).groupByKey().collect()[0][1]), '\n') #collect all values for each key, not do anything yet. Use reduceByKey() to assign only one value for each key.

y = sc.parallelize([(2,3), (0,3), (-1, 7), (2,9)])
print ('\n', y.reduceByKey(lambda x, y: x+y).collect(), '\n')
print ('\n', y.sortByKey().collect(), '\n')

print ('\n', y.mapValues(lambda x: (x,1)).reduceByKey(lambda x, y: (x[0]+y[0], y[1]+x[1])).mapValues(lambda x: x[0]/x[1]).collect(), '\n') #mapValues() only changes the value not the key.
print ('\n', y.lookup(2), '\n') #return the values for the key.



#======== 4 =========
#practice non-pair RDD
x = sc.parallelize([2, 2, 3, 3, 0, 3, 0, -2, 9, 9, -23]) #create a RDD with a list or a list of tuples (pair RDD)

print ('\n', x.first(), x.take(3), x.top(3), '\n') #the first element, 3 elements, and largest 3 elements
print ('\n', x.distinct().collect(), '\n') #only return the distinct values
print ('\n', x.map(lambda x: x*x).collect()) #map() returns RDD with the same size as the original RDD
print ('\n', x.filter(lambda x: x>0).collect()) # filter() returns fewer # of elements

y = sc.parallelize(['abcd', 'efghi'])
print ('\n', y.flatMap(lambda x: [i for i in x]).collect(), '\n') #flatMap() returns more elements
print ('\n', x.cartesian(y).collect(), '\n') #returns cartesian product

z = sc.parallelize([2,3,0,3,3,2])
print ('\n', x.union(z).collect(), '\n', x.intersection(z).collect(), '\n', x.subtract(z).collect(), '\n') #simply concatenate, only contain the sharing elements, remove the sharing elements 

print ('\n', sc.parallelize([2,3,4]).reduce(lambda x,y: x*y), '\n')
print ('\n', x.countByValue().items(), '\n')
print ('\n', sc.parallelize(x.countByValue().items()).collect(), '\n')

x.foreach(lambda x: print ('\n', 'this: ', x**2, '\n'))

print ('\n', sc.parallelize(x.countByValue().items()).map(lambda x: (x[1], x[0])).sortByKey().collect(), '\n')




#======== 3 =======
#This is to find the average number of friends for each age
from pyspark import SparkConf, SparkContext
import statistics

conf = SparkConf().setMaster('local[*]').setAppName('test_mapValues')
sc = SparkContext(conf = conf)

lines = sc.textFile('fakefriends.csv')

def parseline(line):
	line_split = line.split(',')
	return (line_split[2], int(line_split[3]))

age_friends = lines.map(parseline)
average_age_friends = age_friends.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])).mapValues(lambda x: x[0]/x[1]).collect()

average_age_friends = sorted(average_age_friends, key = lambda x: x[1])
print ('\n\n\n====')
for result in average_age_friends:
	print (result)

print ('====\n\n\n')

age_friends_gbk = age_friends.groupByKey().collect()
for i in range(len(age_friends_gbk)):
	age_friends_gbk[i] = list(age_friends_gbk[i]) #tuple is immutable, so convert to list
	age_friends_gbk[i][1] = statistics.mean(age_friends_gbk[i][1])

average_age_friends_gbk = sorted(age_friends_gbk, key = lambda x: x[1])
for result in average_age_friends_gbk:
	print (result)



#======= 2 =======
#This is to practice mapValues() and reduceByKey()
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster('local').setAppName('test_mapValus')
sc = SparkContext(conf = conf)

lines = sc.textFile('1_movie_rating/ml-100k/u1.base')
def parseline(line):
	line_split = line.split()
	return (line_split[0], int(line_split[2]))

user_ratings = lines.map(parseline)

user_ratings = user_ratings.groupByKey().collect() #return a list
print ('\n\n\n======')
print (len(user_ratings))

user_ratings_sorted = sorted(user_ratings, key=lambda x: int(x[0]))
for item in user_ratings_sorted:
	print (item[0], len(item[1]))#one way to find out the number of values for each key

#use mapValues()
user_ratings = lines.map(parseline)
ave_ratings = user_ratings.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])).mapValues(lambda x: x[0]/x[1]).collect()

ave_ratings = sorted(ave_ratings, key = lambda x: x[1]) #sort the list in rating ascending order
for item in ave_ratings:
	print (item)

print ('======\n\n\n')


#======= 1 =======
#in standalone system
from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster('local').setAppName('my_test')
sc = SparkContext(conf=conf)

lines = sc.textFile('1_movie_rating/ml-100k/u1.base')
ratings = lines.map(lambda line: line.split()[1])
result = ratings.countByValue() #result is a dictionary

result_sorted = sorted(result.items(), key=lambda x: x[1]) #sort by value
print (type(result_sorted))
for i in result_sorted:
	print (i)


"""
