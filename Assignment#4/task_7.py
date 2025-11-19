from pyspark.sql import SparkSession
from pyspark.sql import Row


spark = SparkSession.builder.appName("Top 20 Popular Superheroes").getOrCreate()


marvel_graph = spark.sparkContext.textFile("Marvel+Graph")


appearances = marvel_graph.flatMap(lambda line: line.split()[1:]).map(lambda superhero_ID: (int(superhero_ID), 1))


appearance_counts = appearances.reduceByKey(lambda x, y: x + y)


marvel_names = spark.sparkContext.textFile("Marvel+Names")


names = marvel_names.map(lambda line: line.split('"')).map(lambda fields: (int(fields[0]), fields[1]))


appearance_df = appearance_counts.toDF(["superhero_ID", "appearances"])
names_df = names.toDF(["superhero_ID", "superhero_name"])


appearance_df.createOrReplaceTempView("appearance_counts")
names_df.createOrReplaceTempView("superhero_names")


query = """
    SELECT n.superhero_name, c.appearances
    FROM superhero_names n
    JOIN appearance_counts c
    ON n.superhero_ID = c.superhero_ID
    ORDER BY c.appearances ASC
    LIMIT 20
"""


top_20_superheroes = spark.sql(query)


top_20_superheroes.show(truncate=False)  


spark.stop()
