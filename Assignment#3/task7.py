from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("lab7-7").setMaster("local")
sc = SparkContext(conf=conf)

# Create the RDD "lines"
marvel_graph = sc.textFile("../Task_6_7/Marvel+Graph")
marvel_names = sc.textFile("../Task_6_7/Marvel+Names")

# Create a RDD in Key Value form for ID:Name
def id_names_rdd(marvel_names):
    id, name = marvel_names.split(" ",1)
    return int(id),name[1:-1]

marvel_names_by_id = marvel_names.map(id_names_rdd)

# New RDD with all occurences
all_occurrences = marvel_graph.flatMap(lambda x: x.split(" "))
# Transform to new RDD without empty values
all_occurrences_f =  all_occurrences.filter(lambda x: x is not "")

#Trasnform RDD to Key-Word Values in the format Superhero: 1
pair_occurence = all_occurrences_f.map(lambda x: (x,1))
# New RDD with coun of occurences
count_occurences = pair_occurence.reduceByKey(lambda x, y: x + y)

sorted_least_famous = count_occurences.sortBy(lambda x:x[1],ascending=True)
output = sorted_least_famous.take(1)

RDD_by_name = marvel_names_by_id.filter(lambda x: x[0] == int(output[0][0]))
name = RDD_by_name.take(1)

print("{} with {} occurences".format(name[0][1],output[0][1]))

    

