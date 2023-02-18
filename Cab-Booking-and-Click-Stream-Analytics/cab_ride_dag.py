from datetime import datetime

from airflow import  DAG
from airflow.contrib.operators.sqoop_operator import SqoopOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

default_dag = {
	'owner':'airflow',
	'start_date':datetime(2020,1,1),
	'end_date':datetime(2020,1,5),
	'catchup':True
}

cab_ride_batch_dag = DAG(
	'cab_ride_batch_dag',
	default_args = default_dag,
	description='cab_ride_batch_dag',
	schedule_interval = '@daily'
)

extract_cab_booking_table = SqoopOperator(
	task_id='extract_cab_booking_table',
	conn_id='sqoop_cab_ride_connection',
	table='bookings',
	cmd_type='import',
	target_dir='hdfs:///data/raw/booking/{{ds}}',
	where="CAST(pickup_timestamp AS Date)='{{ds}}'",
	split_by='booking_id',
	num_mappers=2,
	dag=cab_ride_batch_dag
)

create_raw_booking_location = BashOperator(
	task_id='create_raw_booking_parent_dir_location',
	bash_command='hdfs dfs -mkdir -p /data/raw/booking',
	dag=cab_ride_batch_dag
)

create_booking_count_location = BashOperator(
        task_id='create_booking_count_location',
        bash_command='hdfs dfs -mkdir -p /data/output/datewise_booking_count',
        dag=cab_ride_batch_dag
)


hive_create_cab_database = "CREATE DATABASE IF 	NOT EXISTS cab_ride"

hql_create_bookings_raw_table = """CREATE TABLE IF NOT EXISTS cab_ride.cab_bookings (
    booking_id STRING,
    customer_id BIGINT,
    driver_id BIGINT,
    customer_app_version STRING,
    customer_phone_os_version STRING,
    pickup_lat DOUBLE,
    pickup_lon DOUBLE,
    drop_lat DOUBLE,
    drop_lon DOUBLE,
    pickup_timestamp TIMESTAMP,
    drop_timestamp  TIMESTAMP,  
    trip_fare BIGINT,
    tip_amount BIGINT,
    currency_code STRING,
    cab_color STRING,
    cab_registration_no STRING,
    customer_rating_by_driver STRING,
    rating_by_customer STRING,
    passenger_count STRING
)
PARTITIONED BY (booking_dt DATE)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','"""

create_hive_booking_database = HiveOperator(task_id='create_hive_booking_database',hql=hive_create_cab_database,
                                       hive_cli_conn_id='hiveserver2_default',dag=cab_ride_batch_dag)

create_hive_raw_booking_table = HiveOperator(task_id='create_hive_booking_table',hql=hql_create_bookings_raw_table,
				       hive_cli_conn_id='hiveserver2_default',dag=cab_ride_batch_dag)

hql_add_partition_for_raw_booking_table = """ALTER TABLE cab_ride.cab_bookings 
	ADD IF NOT EXISTS PARTITION (booking_dt='{{ds}}')
	LOCATION 'hdfs:///data/raw/booking/{{ds}}'"""

add_partition_raw_booking_table = HiveOperator(task_id='add_partiton_to_raw_booking',hql=hql_add_partition_for_raw_booking_table,
                                       hive_cli_conn_id='hiveserver2_default',dag=cab_ride_batch_dag)

hql_create_hive_cab_booking_count_table = """CREATE TABLE IF NOT EXISTS cab_ride.cab_booking_count(
					total_booking_count INT
					)
					PARTITIONED BY (booking_dt DATE)
					STORED AS PARQUET
					LOCATION 'hdfs:///data/output/datewise_booking_count'"""

create_hive_cab_booking_count_table = HiveOperator(task_id='create_hive_cab_booking_count_table',hql=hql_create_hive_cab_booking_count_table,
                                       hive_cli_conn_id='hiveserver2_default',dag=cab_ride_batch_dag)

booking_count_spark_job = SparkSubmitOperator(task_id='date_wise_booking_count_spark_job',
					application='/home/ec2-user/cab_ride_pipeline/aggregate_booking.py',
					conn_id='spark_default',
					spark_binary='spark2-submit',
					application_args=['{{ds}}'],
					dag=cab_ride_batch_dag)

create_raw_booking_location>>extract_cab_booking_table>>create_hive_booking_database

create_hive_booking_database>>create_hive_raw_booking_table>>add_partition_raw_booking_table

add_partition_raw_booking_table>>create_booking_count_location>>create_hive_cab_booking_count_table

create_hive_cab_booking_count_table>>booking_count_spark_job
