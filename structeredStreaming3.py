#read data from rest api and load this in kafka broker(topic) and consume this data from kafka topic
#and load this in mysql db
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
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "livedemo1") \
        .option("startingOffsets", "earliest") \
        .load()
    res = kafka_df.selectExpr("CAST(value AS STRING)")
    #create schema for json data from existiong/dummy json data present in nifioutput folder
    sch = spark.read.format("json").option("multiLine", "true").load("E:\\Datasets\\nifioutput").schema
    fdf = res.withColumn("value", from_json(col("value"), sch))
    fdf.printSchema()

    # #convert this json data into structured format
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


    df1 = flatten(fdf)
    #display data on console
    # df1.writeStream.format('console').outputMode("append").start().awaitTermination()
    # df1.printSchema()


    # write this data in mysql
    def foreach_batch_function(df, epoch_id):
        df.write.mode('append').format('jdbc').option('url','jdbc:mysql://karandxxxxx.ap-south-1.rds.amazonaws.com:3306/mysqldb?useSSL=false') \
            .option('user', 'xxxxx').option('password', 'xxxxxxxxx').option('dbtable', 'livefiledata2'). \
            option('driver', 'com.mysql.jdbc.Driver').save()


    pass

    df1.writeStream.foreachBatch(foreach_batch_function).start().awaitTermination()

