from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("lab7-4").setMaster("local")
sc = SparkContext(conf=conf)

# Create the RDD "lines"
lines = sc.textFile("../Task_4_5/customer-orders.csv")

def parse(lines):
    customer_id, _, ammount = lines.split(",")
    return customer_id, float(ammount)

#New RDD with data in key value pairs    
data = lines.map(parse)

ammount_by_costumer = data.reduceByKey(lambda x, y: x + y)

output = ammount_by_costumer.collect()

for out in output:
    print("Customer_{}: {}".format(out[0],out[1]))




