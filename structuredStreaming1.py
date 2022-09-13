###################
#Read data from kafka broker ,process in spark and write this data into mysql db
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("test").getOrCreate()

sc = spark.sparkContext
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "abcd") \
  .load()
ndf=df.selectExpr("CAST(value AS STRING)")
ndf.printSchema()
#display message on console
# query = ndf.writeStream.format("console").start()
# query.awaitTermination()
#convert this unstructured data into structured format to create dataframe .....use regular expression
ex=r'^(\S+),(\S+),(\d+)'
res=ndf.select(regexp_extract('value',ex,1).alias("name"),regexp_extract('value',ex,2).alias("age"),regexp_extract('value',ex,3).alias("city"))
#res.writeStream.format("console").start().awaitTermination()
###write this into mysql db
def foreach_batch_function(df, bid):
  host="jdbc:mysql://karandb.cnhtjdwvatxj.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSL=false"
  df.write.mode("append").format("jdbc").option("url",host).option("user","myuser")\
    .option("password","mypassword").option("driver","com.mysql.jdbc.Driver")\
    .option("dbtable","kafkadata").save()

  pass

res.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()