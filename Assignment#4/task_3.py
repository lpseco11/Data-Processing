from pyspark.sql import SparkSession


spark = SparkSession.builder.appName("Word Frequency with SQL").getOrCreate()


book = spark.sparkContext.textFile("book")


words = book.flatMap(lambda line: line.split(" ")).filter(lambda word: word.strip() != "")


word_counts = words.map(lambda word: (word.lower(), 1)).reduceByKey(lambda a, b: a + b)


word_frequency_df = word_counts.toDF(["word", "frequency"])


word_frequency_df.createOrReplaceTempView("word_frequency")


query = """
    SELECT word, frequency
    FROM word_frequency
    ORDER BY frequency DESC
"""


sorted_word_frequency_df = spark.sql(query)


sorted_word_frequency_df.show(truncate=False)


spark.stop()
