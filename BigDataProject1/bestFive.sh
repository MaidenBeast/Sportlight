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

$HADOOP_HOME/bin/hdfs dfs -put $HOME/inputdata/example.txt input

$HADOOP_HOME/bin/hadoop jar $HOME/BigDataProject1.jar radeon/BestFivePerMonth input/example.txt output/result_prova

$HADOOP_HOME/bin/hdfs dfs -get output/result_prova $HOME/output

### ...your job goes here...

# Stop HADOOP services
$MYHADOOP_HOME/bin/myhadoop-shutdown.sh