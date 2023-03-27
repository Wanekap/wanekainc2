import pyspark
import pyspark.sql.types as T
from operator import add

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.functions import lit
import pyspark.sql.functions as func


from pyspark.sql import functions as F
from pyspark.sql.functions import concat,concat_ws
from pyspark.sql.functions import ceil, col



spark = SparkSession.builder.master("local").appName("PySpark_Postgres_test").getOrCreate()
dburl="jdbc:postgresql://ec2-13-40-49-105.eu-west-2.compute.amazonaws.com:5432/testdb"

max = spark.sql("select max(coin_id) from pythongroup.wanekaterraluna1 as max")

max=max.first()['max(coin_id)']

query="(select * from wanekaterraluna1 where coin_id >"+str(max)+ ") as tb"
df = spark.read.format("jdbc").option("url",dburl) \
    .option("driver", "org.postgresql.Driver").option("dbtable", query) \
    .option("user", "consultants").option("password", "WelcomeItc@2022").load()

print(df.show())

#intdf = df.select("*",ceil("price")).show()
intdf = df.withColumn("roundedprice", func.round(df["price"], 2))

#csdf = df.withColumn("circulating_supply", col("market_cap') / col("price"))
csdf = df.withColumn("circulating_supply", df.market_cap/df.price)



# Create Hive table
intdf.write.mode('append') \
    .saveAsTable("pythongroup.wanekaterraluna1")

csdf.write.mode('append') \
    .saveAsTable("pythongroup.wanekaterraluna2")
