# Databricks notebook source
#SparkContext
sc

# COMMAND ----------

# do word count
# create rdd from text file
wordsRdd = sc.textFile("/FileStore/tables/words.txt")

# COMMAND ----------

print("Lines count ", wordsRdd.count()) # count is action function
# lambda line: line != "", is a function, accept a string, a line as input, returns True or False 
# filter is transform function, if the condition matches, it picks the line/input, it drop it
# for every line, filter calls lambda with line as input, based on function return value True/False, it picks or drop it

# strip remove space before and after the string
# map is transform function, accept line as input, remove the spaces around, return the cleaned line
# stripSpaceRdd is a child rdd, from wordsRdd
stripSpaceRdd = wordsRdd.map (lambda line: line.strip())
# the filter function shall receive the lines which spaces removed around the line string
nonEmptyLinesRdd =  stripSpaceRdd.filter (lambda line: line != "") # filter is a transform function
print("non empty lines ", nonEmptyLinesRdd.count())

# COMMAND ----------

def printLine(line):
  print ("***"+line+"***")
  
#nonEmptyLinesRdd.foreach(printLine)
# collect an action method, it gets the result from all works to driver/this application
# cleanedLines is list, not a rdd
cleanedLines = nonEmptyLinesRdd.collect() 
print(cleanedLines)

# COMMAND ----------

# map is transform, convert one format to another format, won't filter
wordListRdd = nonEmptyLinesRdd.map (lambda line: line.split(" "))
# we will list of list of word
print (wordListRdd.collect())
print(wordListRdd.count())

# COMMAND ----------

# convert the list of words into word
# flatMap, it flatten the list, convert list of elements into element
wordsRdd = wordListRdd.flatMap(lambda arr: arr)
print(wordsRdd.collect())
print(wordsRdd.count())

# COMMAND ----------

filteredWordRdd = wordsRdd.filter (lambda line: line != "")
print(filteredWordRdd.collect())
print(filteredWordRdd.count())

# COMMAND ----------

# set the transformation, bring a structure to the word as tuple (word, occurance) (spark, 1)
# keyed RDD, (key, value), (word, occurance), (spark, 1)

wordOccuranceRdd = filteredWordRdd.map (lambda word: (word, 1) )
print(wordOccuranceRdd.collect())

# COMMAND ----------

"""
lambda accumulator, occurance:  accumulator +  occurance is called by spark reduceByKey

If the word repeats only once, it takes as accoumulator = value
on every second occurance of the word

Input
('python', 1)  <-- won't call lambda since python first time
('scala', 1)    <-- won't call lambda since scala first time
('python', 1)  <-- now it will call lambda, since python is repeating, the value present in accumulator
                     lambda (1/acc, 1/occ) = 1 + 1 = 2
('python', 1)  <->  calls lambda lambda (2/acc, 1/occ) = 2 + 1 = 3
Table-/accomulator
word       count/accomulator
python     3
scala      1    

"""

wordCountRdd = wordOccuranceRdd.reduceByKey(lambda accumulator, occurance:  accumulator +  occurance)
print(wordCountRdd.collect())

# COMMAND ----------


