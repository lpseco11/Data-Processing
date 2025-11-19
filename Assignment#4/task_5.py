from pyspark.sql import SparkSession
from pyspark.sql import Row


spark = SparkSession.builder.appName("Total Amount Spent by Customer").getOrCreate()


data = spark.sparkContext.textFile("customer-orders.csv")


def mapper(line):
    fields = line.split(",")
    return Row(
        customer_ID=str(fields[0]),
        product_id=str(fields[1]),
        price=float(fields[2])  
    )


transactions = data.map(mapper)


transactions_df = spark.createDataFrame(transactions)


transactions_df.createOrReplaceTempView("transactions")


total_spent_query = """
    SELECT 
        customer_ID, 
        ROUND(SUM(price), 2) as total_amount_spent
    FROM transactions
    GROUP BY customer_ID
    ORDER BY total_amount_spent DESC
"""

total_spent = spark.sql(total_spent_query)

total_spent.show()

spark.stop()
