# AWS-Snowflake-Pipeline-using-Kinesis-and-Airflow
I ingest data using kinesis firehose and send to s3 then move  the proceesed data to snowflake .I automate the process with airflow

## Architecture
![](https://github.com/d-owusu/AWS-Snowflake-Pipline-using-KInesis-and-Airflow/blob/main/Architecture.png)

## EC2
I create an EC2 instance. I use the small free tier instance which was able able to handle the task .I attach an IAM role with full access to firshose and  cloudwatch . Install the kinesis firehose agent.
![](https://github.com/d-owusu/AWS-Snowflake-Pipline-using-KInesis-and-Airflow/blob/main/pictures/install_kinesis_agent.png)

## Firehose
I create two delivery streams. One for customer data and one for Order data
![](https://github.com/d-owusu/AWS-Snowflake-Pipline-using-KInesis-and-Airflow/blob/main/pictures/kinesis_delivery_streams.png)

## Firehose
Edit the  agent.json file to reflect the the file locations and names of of delivery streams 
![](https://github.com/d-owusu/AWS-Snowflake-Pipline-using-KInesis-and-Airflow/blob/main/pictures/kinesis_configuration.png)

##  Start Firehose Agent
Start firshose-agent with **sudo service aws-kinesis-agent start** . Use **tail -f /var/log/aws-kinesis-agent/aws-kinesis-agent.log** to check the logs.
**NB: The kinesis agent configuration file is sensitive to case letters**
From the picture, all recoreds for customers and orders were successfully parsed.
![](https://github.com/d-owusu/AWS-Snowflake-Pipline-using-KInesis-and-Airflow/blob/main/pictures/kinesis_success.png)

## s3
Data can be seen be in S3
![](https://github.com/d-owusu/AWS-Snowflake-Pipline-using-KInesis-and-Airflow/blob/main/pictures/s3.png)
