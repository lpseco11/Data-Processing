from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("lab7-5").setMaster("local")
sc = SparkContext(conf=conf)

# Create the RDD "lines"
lines = sc.textFile("../Task_4_5/customer-orders.csv")

def parse(lines):
    customer_id, _, ammount = lines.split(",")
    return customer_id, float(ammount)

#New RDD with data in key value pairs    
data = lines.map(parse)

ammount_by_costumer = data.reduceByKey(lambda x, y: x + y)

sorted_rdd = ammount_by_costumer.sortBy(lambda x:x[1],ascending=True)

output = sorted_rdd.collect()

for out in output:
    print("Customer_{}: {}".format(out[0],out[1]))




