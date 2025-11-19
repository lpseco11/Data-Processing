from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("lab7-1").setMaster("local")
sc = SparkContext(conf=conf)

# Create the RDD "lines"
lines = sc.textFile("../Task_1/1800.csv")

#Function to parse the RDD into tuples (stationID, entryType, temperature)
def parseLine(line):
    stationID, _,entryType,temperature,*_ = line.split(',')
    return (stationID, entryType, temperature)


#Maps new RDD based on the parse function
parsedLines = lines.map(parseLine)

#Narrow transformation to trim the uneeded information in the RDD (the non TMIN temperature values)
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])

#New RDD with only StationID and Temperature
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))

results = minTemps.collect()

for result in results:
    print(result)