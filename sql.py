from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .appName("MostSoldProduct") \
        .getOrCreate()

sales_data = [
    ("Samsung S5",),
    ("iPhone X",),
    ("Samsung S5",),
    ("Samsung S5",),
    ("iPhone X",),
]

df = spark.createDataFrame(sales_data, ["pname"])
df.createOrReplaceTempView("sales_data")

most_sold_product_query = """
    SELECT pname, COUNT(pname) as sold_count
    FROM sales_data
    GROUP BY pname
    ORDER BY sold_count DESC
    LIMIT 1
"""

most_sold_product_df = spark.sql(most_sold_product_query)
most_sold_product_df.show()

spark.stop()