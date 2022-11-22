import os
from airflow import DAG 
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import datetime

#S3_CONN_ID = S3Hook(aws_conn_id= 'aws_default')
SNOWFLAKE_CONN_ID = 'snowflake_conn'

default_args = {
    "owner" : "snowflakedatapipeline",
    "depends_on_past" : False,
    "start_date" : days_ago(1),
    "retries" : 0,
    "retry_delay" : timedelta(minutes=5)
}

dag = DAG (
    'shopdb_customers_orders_datapipeline',
    default_args=default_args,
    description= 'Runs Data Pipeline',
    schedule_interval= None,
    is_paused_upon_creation= False,

)


bash_task = BashOperator(task_id = 'run_bash_echo', bash_command='echo 1', dag =dag)
post_task = BashOperator(task_id = 'end_task', bash_command='echo 0 ', dag = dag )

batch_id = str(datetime.datetime.now().strftime('%Y%m%d%H%M'))
print("Batch_ID = " + batch_id)

#CUSTOMERS

task_customers_landing_to_processing = BashOperator(
 task_id="customer_landing_to_processing",
 bash_command='aws s3 mv s3://snflakedata/firehose/customers/landing/ s3://snflakedata/firehose/customers/processing/{0}/ --recursive'.format(batch_id),
 dag=dag
)
 
task_customers_processing_to_processed = BashOperator(
 task_id="customer_processing_to_processed",
 bash_command='aws s3 mv s3://snflakedata/firehose/customers/processing/ s3://snflakedata/firehose/customers/processed/{0}/ --recursive'.format(batch_id),
 dag=dag
)




#ORDERS
task_orders_landing_to_processing = BashOperator(
    dag=dag,
    task_id = "orders_landing_to_processing" ,
    bash_command = 'aws s3 mv s3://snflakedata/firehose/orders/landing/ s3://snflakedata/firehose/orders/processing/{0}/ --recursive'.format(batch_id),
     
)



task_orders_processing_to_processed = BashOperator(
    task_id = "orders_processing_to_processed" ,
    bash_command = 'aws s3 mv s3://snflakedata/firehose/orders/processing/ s3://snflakedata/firehose/orders/processed/{0}/ --recursive'.format(batch_id),
    dag=dag
)

#LOAD DATA
load_orders_snowflake = [
    """ copy into orders_raw
(O_ORDERKEY,O_CUSTKEY,O_ORDERSTATUS,O_TOTALPRICE,O_ORDERDATE,O_ORDERPRIORITY,O_CLERK,O_SHIPPRIORITY,O_COMMENT, O_BATCH_ID) from
( select t.$1,t.$2,t.$3,t.$4,t.$5,t.$6,t.$7,t.$8,t.$9, '20221114020201' from @ORDER_RAW_STAGE t)  ON_ERROR = 'continue';""".format(batch_id),

]

load_customers_snowflake = [
    """copy into CUSTOMERS_RAW
(C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT, C_BATCH_ID) from
( select t.$1,t.$2,t.$3,t.$4,t.$5,t.$6,t.$7,t.$8,'20221114020201' from @CUSTOMER_RAW_STAGE t) ON_ERROR = 'continue';""".format(batch_id),
]


snowflake_query_customers_orders_transformation = [
    """ INSERT INTO ORDER_CUSTOMER_DATE_PRICE
    (customer_name, order_date, order_total_price, batch_id)
    (SELECT c.c_name as customer_name , o.o_orderdate as order_date, sum(o.o_totalprice) as order_total_price, c_batch_id
from orders_raw o join customers_raw c on o.o_custkey = c.c_custkey and o_batch_id = c_batch_id
where o_orderstatus = 'F'
group by c_name, o_orderdate , c_batch_id
order by o_orderdate);"""
    
]



snowflake_orders_sql_str = SnowflakeOperator(
    task_id='insert_raw_orders_snowflake',
    dag=dag,
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=load_orders_snowflake,
    warehouse="SHOP_WH",
    database="SHOP_DB",
    schema="SHOP_SCHEMA",
    role="ACCOUNTADMIN",
)

snowflake_customers_sql_str = SnowflakeOperator(
    task_id='insert_raw_customers_snowflake',
    dag=dag,
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql=load_customers_snowflake,
    warehouse="SHOP_WH",
    database="SHOP_DB",
    schema="SHOP_SCHEMA",
    role="ACCOUNTADMIN",
)


snowflake_customers_order_transformation= SnowflakeOperator(
    task_id='order_customer_transformation',
    dag=dag,
    snowflake_conn_id=SNOWFLAKE_CONN_ID,
    sql= snowflake_query_customers_orders_transformation,
    warehouse="SHOP_WH",
    database="SHOP_DB",
    schema="SHOP_SCHEMA",
    role="ACCOUNTADMIN",
)






[bash_task >>  task_customers_landing_to_processing >> snowflake_customers_sql_str >> task_customers_processing_to_processed ,
 bash_task >>task_orders_landing_to_processing >> snowflake_orders_sql_str >> task_orders_processing_to_processed] >> snowflake_customers_order_transformation >> post_task
