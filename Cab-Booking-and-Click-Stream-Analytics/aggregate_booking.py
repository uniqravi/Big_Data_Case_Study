#!/usr/bin/env python
# coding: utf-8

# In[8]:


import os
import sys
os.environ["PYSPARK_PYTHON"] = "/opt/cloudera/parcels/Anaconda/bin/python"
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_232-cloudera/jre"
os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/SPARK2-2.3.0.cloudera2-1.cdh5.13.3.p0.316101/lib/spark2/"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.10.6-src.zip")
sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")


# In[15]:


from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr,col


# In[9]:


spark=SparkSession.builder.appName('datewise_total_bookings')     .config("spark.sql.sources.partitionOverWriteMode","dynamic")    .config("hive.exec.dynamic.partition","true")     .config("hive.exec.dynamic.partition.mode","nonstrict")     .config("spark.sql.shuffle.partitions",5)     .getOrCreate()


# In[10]:


run_date=sys.argv[1]
#run_date = '2020-01-05'


# In[11]:


bookings=spark.table("cab_ride.cab_bookings")     .filter(expr("booking_dt = '{}'".format(run_date)))


# In[13]:


#bookings.select('booking_id','customer_id').show() //correct way


# In[17]:


#bookings.select(col('booking_id'),col('customer_id')).show() //correct way


# In[12]:


#bookings.show() //correct way


# In[23]:


## date wise count,
bookings.groupBy(col('booking_dt')).count().select('booking_dt','count')     .withColumnRenamed('count','total_booking_count').createOrReplaceTempView("booking_count_result")


# In[24]:



#spark.sql('select * from booking_count_result')


# In[ ]:


## save aggregate data for specific run date 
## we will insert data into selective partitions
insert_aggregate_hql = """INSERT OVERWRITE TABLE cab_ride.cab_booking_count PARTITION (booking_dt) 
    SELECT * FROM booking_count_result"""
spark.sql(insert_aggregate_hql)

