from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Minimum Temperature").getOrCreate()

data = spark.sparkContext.textFile("1800.csv")

def mapper(line):
    fields = line.split(",")
    return Row(
        Station=str(fields[0]),
        Date=str(fields[1]),
        Name=str(fields[2]),
        Temperature=float(fields[3])  
    )


weather = data.map(mapper)


weather_df = spark.createDataFrame(weather)


weather_df.createOrReplaceTempView("weather_data")


min_temp_by_station = spark.sql("""
    SELECT Station, MIN(Temperature) as MinTemperature
    FROM weather_data
    GROUP BY Station
    ORDER BY MinTemperature
""")

min_temp_by_station.show()

spark.stop()
