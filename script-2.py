from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import split
import collections
spark =SparkSession.builder.appName("SomatoriosCaracter").getOrCreate()
def mapper(line):
 fields = line.split(",")
 return Row(en=fields[1],pt=fields[2],rate=fields[-1])
lines = spark.sparkContext.textFile("imdb-reviews-ptbr.csv")
reviews = lines.map(mapper)
schema = spark.createDataFrame(reviews).cache()
schema.createOrReplaceTempView("reviews")
df_pt = spark.sql("""select pt from reviews where rate='"neg"'""").collect()
df_en= spark.sql("""select en from reviews where rate='"neg"'""").collect()
resultado_en=0
for element in df_en:
	splited_str = str(element)
	identificadorRU3678100 = splited_str.split()
	x = len(identificadorRU3678100)
	resultado_en = resultado_en+ x
resultado_pt=0
for element in df_pt:
	splited_str = str(element)
	identificadorRU3678100 = splited_str.split()
	x = len(identificadorRU3678100)
	resultado_pt= resultado_pt+ x
print("#######################################################")
print("%d palavras em ingles" % resultado_en)
print("%d palavras em portugues" % resultado_pt)
final=resultado_pt-resultado_en
print("Portugues tem %d palavras a mais" % final)
print("#######################################################")
spark.stop()