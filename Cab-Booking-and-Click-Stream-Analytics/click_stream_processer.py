#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import sys
os.environ["PYSPARK_PYTHON"] = "/opt/cloudera/parcels/Anaconda/bin/python"
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_232-cloudera/jre"
os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/SPARK2-2.3.0.cloudera2-1.cdh5.13.3.p0.316101/lib/spark2/"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.10.6-src.zip")
sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")


# In[2]:


from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr,col
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,FloatType,DoubleType,ArrayType,LongType
from pyspark.sql.functions import window,to_timestamp,from_json,col,unix_timestamp,collect_list,struct,udf
from pyspark.sql.functions import array,explode,size,min,max,count,sum


# In[ ]:


inputDF = spark.readStream.format("kafka")    .option("kafka.bootstrap.servers","18.211.252.152:9092")    .option("subscribe","de-capstone3")    .option("startingOffsets",'earliest')    .load().repartition(10)


# In[ ]:


QUERY_NAME = 'CLICK_STREAM_PROCESSOR'


# In[ ]:


schema = StructType([StructField("customer_id",StringType(),True),
                     StructField("app_version",StringType(),True),
                     StructField("os_version",StringType(),True),
                     StructField("lat",StringType(),True),
                     StructField("lon",StringType(),True),
                     StructField("page_id",StringType(),True),
                     StructField("is_button_click",StringType(),True),
                     StructField("is_page_view",StringType(),True),
                     StructField("is_scroll_up",StringType(),True),
                     StructField("is_scroll_down",StringType(),True),
                     StructField("timestamp",StringType(),True),
                    ])


# In[ ]:


lines = inputDF.selectExpr("cast(value as string)")

final_df = lines.withColumn('json_data',from_json(col('value'),schema)).select("json_data.*")


# In[ ]:


outputStream = final_df         .writeStream         .format()         .outputMode("append")         .option("truncate", False)         .option("path", "/data/clickstream/result/")         .option("checkpointLocation", "/data/clickstream/checkpoint/")         .trigger(processingTime="5 minute")         .queryName(QUERY_NAME)

query=outputStream.start()
query.awaitTermination()

