from pyspark.sql importSparkSession
from pyspark.sql import Row
from pyspark.sql.functions import split
import collections
spark =SparkSession.builder.appName("SomatoriosID").getOrCreate()
def identificador3678100(line):
 fields = line.split(",")
 return Row(ids=fields[0],rate=fields[-1])

lines = spark.sparkContext.textFile("imdb-reviews-ptbr.csv")
reviews = lines.map(identificador3678100)
schema = spark.createDataFrame(reviews).cache()
schema.createOrReplaceTempView("reviews")
result = spark.sql("select rate,sum(regexp_replace(ids,'[^0-9]','')) from reviews group by rate").show()
spark.stop()