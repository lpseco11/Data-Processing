from pyspark.sql import SparkSession
from pyspark.sql import Row


spark = SparkSession.builder.appName("Word Frequency").getOrCreate()


data = spark.sparkContext.textFile("Book")


def mapper(line):
    words = line.split()  
    return [Row(word=word.lower()) for word in words]  


words_rdd = data.flatMap(mapper)


words_df = spark.createDataFrame(words_rdd)


words_df.createOrReplaceTempView("words_data")


word_frequency = spark.sql("""
    SELECT word, COUNT(*) as frequency
    FROM words_data
    GROUP BY word
""")

word_frequency.show(n=100,truncate=False)

spark.stop()
