
# Cab Ride Bookings & Click-Stream Analysis

### Introduction

Suppose company built system based on OLTP (on-Line Transaction Processing) . Since role of OLTP system is to provide better customer experience. OLTP System should be consistent, highly available and faster response time and backend storage of such system follows relational modelling (Normalisation) to update existing records, querying the data to reduce anomalies.

But with OLTP system , It is very hard to get business numbers which are very useful to derive quick and on demand insights to decide the company's future strategy. Owing to the massive growth in business, it is getting tough for the company's management to obtain the business numbers frequently, as backend performing operations on database are not capable of catering to all types of queries owing to the large volume of data.

So Company also want to build  OLAP based system so that business stack-holders are able to analyse quickly. To analyse , data need to store in Data Warehouse by following proper Dimension Modelling with defined structured format.
Here, It is a end to end big data solution from starting ingesting data into data warehouse (Hive/HDFS) and doing analysis using PySpark. These pipeline has been orchestrated using Airflow.

### Goals

##### Booking data analytics solution: 
This is a feature in which the ride-booking data is stored in a way such that it is available for all types of analytics without hampering business. These analyses mostly include daily, weekly, and monthly booking counts as well as booking counts by the mobile operating system, average booking amount, total tip amount, etc.

##### Clickstream data analytics solution: 
Clickstream is the application browsing data that is generated on every action taken by the user on the app. This includes link click, button click, screen load, etc. This is relevant to know the user's intent and then take action to engage them more in the app according to their browsing behavior. Since this is very high-volume data (more than the bookings data), it needs to be architecture quite well and stored in a proper format for analysis.

### Data Details
1. Customer Bookings: Customer actual successful booking ride information is stored in the booking table which is in the RDBMS MySQL database. This table contains the following columns

`
booking_id, customer_id, driver_id, customer_app_version, customer_phone_os_version, pickup_lat, pickup_lon, drop_lat, drop_lon, pickup_timestamp, drop_timestamp,trip_fare, tip_amount,currency_code, cab_color, cab_registration_no, customer_rating_by_driver, rating_by_customer, passenger_count
`

2. Click_Stream : It is stream data of user activity such as click, view and page load which is form of json format and contains following columns


`
customer_id, app_version, os_version, lat, lon, page_id, button_id, is_button_click, is_page_view, is_scroll_up, is_scroll_down, timestamp
`

### Big Data Approach/Solution



## Screenshots

![App Screenshot](fig1.jpg)

Since System should able to handle and process streaming data as well as batch data.

#### Stream Pipeline: 

As mentioned earlier in the problem statement, This cab company stores the click-stream information. So Each real-time event of user-interactions in the cab app like button-click,page-view, etc. are fed into Kafka messaging system. 
A Spark streaming engine that continuously keeps triggering job at a regular interval, consumes events from Kafka and process further those events. It converts JSON into data frame format and applies validations and transformation which is useful for further analysis. Then It sinks the converted consumed events into HDFS location.

#### Batch Pipeline: 
Since daily successful bookings keep coming in Amazon RDS. So our batch pipeline should handle incremental transactional data on daily basis.

Here, first, we should create necessary hdfs locations and hive tables with necessary partitions. These locations and hive tables will be used to load source data and the aggregated result of the spark job.

then, after configuring Sqoop import job, it keeps ingesting daily data into hdfs location. Since we are handing incremental data. So Sqoop loads data into the datewise folder. 

But still, we cannot perform hive queries on those newly added data. So new datewise folder is added to the hive meta-store after running the hive DDL alter table. This DDL command will add the newly created date folder as a new partition folder of the particular table into the meta-store.

After successful loading of booking data into hdfs location, then spark job runs and read data newly added date folder data and perform some aggregation transformation and calculate information like total bookings, total booking amount, top tip amount for the same date. Then it overwrites this aggregate result into a specific booking date partition of the hive table.

Here, we used Airflow to orchestrate the batch pipeline. A sequence of operations is connected. Next operations only get triggered after a successful previous operation. So airflow is the better option here, It creates Dag to manage the interdependence of tasks. Dag is formed among interconnections of multiple operators. Operators are basic operations like bash operator, hive operator, and Sqoop operator. 

We use SqoopOperator, HiveOperator, BashOperator, and SparkJobSumitterOperator in Airflow

SqoopOperator → it runs Sqoop job on scheduled date which is usually a daily date.

HiveOpertator → It performs HQL queries for the creation of table and partition addition.

BashOperator → It is responsible to perform the bash command, here we use it for the creation hdfs location.

SparkJobSubmiiter -> it is responsible to submit spark job to calculate the aggregated results.


### Code Walk-Through
Code section can be divided into three part

part-1 : Spark-Streaming Python Code Responsible for Consuming click-stream, doing some transformations and sinking

part-2 : Airflow DAG Code Responsible for to run series of Operations in Batch- Pipeline on Daily Basis

part-3 : Spark Python Source Code for aggregate calculation

### Conclusion

This case study provides to good opportunity to learn big data concept using popular technologies like Spark, Hive, Kafka, Hadoop,Airflow. We can also understand end to end streaming and batch-line and ELT & ETL process.
