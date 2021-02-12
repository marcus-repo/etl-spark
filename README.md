# 1) Introduction

The startup company Sparkify launched a new music streaming app and wants to get insights about the music preferences of their users, in particular Sparkify wants to understand what songs users are listening to.

The goal of this project is to build a Datalake on AWS S3 which will serve as a single source of truth for data analysis. An AWS EMR Spark Cluster serves as an ETL pipeline and feeds the Sparkify AWS S3 Datalake.

# 2) Repository
### Structure:

**Source Data:** </br>
*AWS S3 Bucket:*
https://s3.console.aws.amazon.com/s3/buckets/udacity-dend
- log_data/
- song_data/

**Files:**
- dwh.cfg
- etl.py
- README.md

### Explanation:
#### Source Data:
- log_data/ and song_data/ contain JSON files collected from the app.</br>

#### Files:
- **dwh.cfg** *configuration file for AWS S3 connection.*
- **etl.py** *python ETL script which reads the JSON sourcefiles and writes the data to AWS S3 in parquet file format.*
- **README.md** *describes the project.*

# 3) AWS S3 and EMR Spark Cluster
The source data as well as the cleaned data resides in AWS S3 Buckets.

### Benefits of Amazon S3 Buckets
- Durability
- Low cost
- Scalability
- Availability
- Security 
- Flexibility
- Simple data transfer

The source data is loaded into an AWS EMR Spark Cluster, cleaned and written back to AWS S3 in parquet file format.
Spark is a general distributed data processing engine built for speed and flexibility.

### Benefits of EMR Spark Cluster
- EMR integrates easily with other AWS services e. g. S3
- Pre-Build Configurations ensure easy deployment
- Scalability and Flexibility, Instances can be added whenever needed
- Out of the box monitoring tools
- Jupyter Notebooks with pyspark kernel faciliate fast development


### S3 Datalake - Analytics directories overview:
Files are stored in parquet format on the datalake. Advantages can be found here:
https://databricks.com/glossary/what-is-parquet


***songs_table.parquet - partitioned by year and month***
- songplays_id
- year
- month
- start_time
- user_id
- level
- song_id
- artist_id
- session_id
- location
- user_agent

***users_table.parquet***
- user_id
- first_name
- last_name
- gender
- level

***songs_table.parquet - partitioned by year and artist_id***
- song_id
- title
- artist_id
- year
- duration

***artists_table.parquet***
- artist_id
- name
- location
- latitude
- longitude

***time_table.parquet - partitioned by year and month***
- start_time
- hour
- day
- week
- month
- year


# 4) ETL Execution

This section explains the execution of the ETL pipeline.
The python script can be copied to an AWS EMR spark master node and excecuted in the terminal.

- launch spark-cluster on AWS EMR
- copy etl.py to master node:  scp -i "XXXXX.pem" "etl.py" hadoop@XXXXXcompute.amazonaws.com:/home/hadoop/
- ssh into master node: ssh -i "XXXXX.pem" hadoop@XXXXXcompute.amazonaws.com
- execute script: /usr/bin/spark-submit --master yarn ./etl.py


# 5) Ressources
https://aws.amazon.com/s3/ </br>
https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-overview-benefits.html


