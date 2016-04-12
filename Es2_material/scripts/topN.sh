#!/bin/bash

#PBS -A train_bigdat16
#PBS -l walltime=00:05:00
#PBS -l select=1:ncpus=20:mem=96GB
#PBS -q parallel

## Environment configuration
module load profile/advanced hadoop/2.5.1
# Configure a new HADOOP instance using PBS job information
$MYHADOOP_HOME/bin/myhadoop-configure.sh -c $HADOOP_CONF_DIR
# Start the Datanode, Namenode, and the Job Scheduler
$HADOOP_HOME/sbin/start-dfs.sh

$HADOOP_HOME/bin/hdfs dfs -mkdir /user
$HADOOP_HOME/bin/hdfs dfs -mkdir /user/a08trb23

$HADOOP_HOME/bin/hdfs dfs -put $HADOOP_HOME/etc/hadoop input

$HADOOP_HOME/sbin/start-yarn.sh

$HADOOP_HOME/bin/hdfs dfs -mkdir output

$HADOOP_HOME/bin/hdfs dfs -put $HOME/example_data/pg201.txt input

$HADOOP_HOME/bin/hadoop jar $HOME/Es2.jar topn/TopN input/pg201.txt output/result

$HADOOP_HOME/bin/hdfs dfs -get output $HOME/output

### ...your job goes here...

# Stop HADOOP services
$MYHADOOP_HOME/bin/myhadoop-shutdown.sh
