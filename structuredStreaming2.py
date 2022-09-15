###################
#live data generated using rest api. using nifi load this into local folder
#Read live data from local folder ,process in spark and write this data into mysql db
from pyspark.sql import *
from pyspark.sql.functions import *
import re
import json
from pyspark.sql.types import *
if __name__=="__main__":
    spark = SparkSession.builder.master("local[*]").appName("test").\
      config("spark.streaming.stopGracefullyOnShutdown","true").\
      config("spark.sql.streaming.schemaInference","true").getOrCreate()
    #read data from nifioutput folder---(using nifi livedata is generated in nifioutput folder)
    raw_df=spark.readStream.format('json').option('multiLine','true').\
      option("path","E:\\Datasets\\nifioutput").option("maxFilesPerTrigger",1).load()
    raw_df.printSchema()

    #convert this json data into structured format
    def read_nested_json(df):
      column_list = []
      for column_name in df.schema.names:
        if isinstance(df.schema[column_name].dataType, ArrayType):
          df = df.withColumn(column_name, explode(column_name))
          column_list.append(column_name)
        elif isinstance(df.schema[column_name].dataType, StructType):
          for field in df.schema[column_name].dataType.fields:
            column_list.append(col(column_name + "." + field.name).alias(column_name + "_" + field.name))
        else:
          column_list.append(column_name)
      df = df.select(column_list)
      return df;


    def flatten(df):
      read_nested_json_flag = True
      while read_nested_json_flag:
        df = read_nested_json(df);
        read_nested_json_flag = False
        for column_name in df.schema.names:
          if isinstance(df.schema[column_name].dataType, ArrayType):
            read_nested_json_flag = True
          elif isinstance(df.schema[column_name].dataType, StructType):
            read_nested_json_flag = True;
      cols = [re.sub('[^a-zA-Z0-1]', "", c.lower()) for c in df.columns]
      return df.toDF(*cols);

    df1 = flatten(raw_df)

    df1.printSchema()
    #display data on console ....just for testing purpose
    #df1.writeStream.format('console').outputMode("append").start().awaitTermination()

    #write this data in mysql
    def foreach_batch_function(df,epoch_id):
      df.write.mode('append').format('jdbc').option('url','jdbc:mysql://karandbxxxx.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSL=false')\
        .option('user','xxxxxx').option('password','xxxxxxxx').option('dbtable','livefiledata').\
        option('driver','com.mysql.jdbc.Driver').save()
    pass

    df1.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()












# def read_nested_json(df):
#   column_list = []
#   for column_name in df.schema.names:
#     if isinstance(df.schema[column_name].dataType, ArrayType):
#       df = df.withColumn(column_name, explode(column_name))
#       column_list.append(column_name)
#     elif isinstance(df.schema[column_name].dataType, StructType):
#       for field in df.schema[column_name].dataType.fields:
#         column_list.append(col(column_name + "." + field.name).alias(column_name + "_" + field.name))
#     else:
#       column_list.append(column_name)
#   df = df.select(column_list)
#   return df
#
#
# def flatten(df):
#   read_nested_json_flag = True
#   while read_nested_json_flag:
#     df = read_nested_json(df)
#     read_nested_json_flag = False
#     for column_name in df.schema.names:
#       if isinstance(df.schema[column_name].dataType, ArrayType):
#         read_nested_json_flag = True
#       elif isinstance(df.schema[column_name].dataType, StructType):
#         read_nested_json_flag = True
#   cols = [re.sub('[^a-zA-Z0-1]', "", c.lower()) for c in df.columns]
#   return df.toDF(*cols)
#
#
# df= spark \
#   .readStream \
#   .format("kafka") \
#   .option("kafka.bootstrap.servers", "localhost:9092") \
#   .option("subscribe", "india2") \
#   .load()
# ndf=df.selectExpr("CAST(value AS STRING)")
# #ndf.printSchema()
# res=flatten(ndf)
#
# res.printSchema()







#display message on console
# query = ndf.writeStream.format("console").start()
# query.awaitTermination()
#convert this unstructured data into structured format to create dataframe .....use regular expression
#ex=r'^(\S+),(\S+),(\d+)'
#res=ndf.select(regexp_extract('value',ex,1).alias("name"),regexp_extract('value',ex,2).alias("age"),regexp_extract('value',ex,3).alias("city"))
#res.writeStream.format("console").start().awaitTermination()
###write this into mysql db
# def foreach_batch_function(df, bid):
#   host="jdbc:mysql://karandb.cnhtjdwvatxj.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSL=false"
#   df.write.mode("append").format("jdbc").option("url",host).option("user","myuser")\
#     .option("password","mypassword").option("driver","com.mysql.jdbc.Driver")\
#     .option("dbtable","kafkadata").save()
#
#   pass
#
# res.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()