from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("lab7-2").setMaster("local")
sc = SparkContext(conf=conf)

# Create the RDD "lines"
lines = sc.textFile("../Task_2_3/Book")
# New RDD with more entries consisiting of all the words
words = lines.flatMap(lambda x: x.split(" "))

# New RDD without non alphabetical words
filtered_words = words.filter(lambda x: x.isalpha())

# New RDD with word key-value pairs
pairs = filtered_words.map(lambda x: (x,1))

#New RDD with Reduce by value
counted_words = pairs.reduceByKey(lambda x, y: y + y)
# Get python array with output
output = counted_words.collect()

for out in output:
    print("{}: {}".format(out[0],+out[1]))


