#!/bin/bash

#PBS -A try16_Roma3
#PBS -l walltime=01:00:00
#PBS -l select=1:ncpus=20:mem=96GB
#PBS -q parallel

## Environment configuration
module load profile/advanced hadoop/1.2.1
# Configure a new HADOOP instance using PBS job information
$MYHADOOP_HOME/bin/myhadoop-configure.sh -c $HADOOP_CONF_DIR
# Start the Datanode, Namenode, and the Job Scheduler 
$HADOOP_HOME/bin/start-all.sh 

$HADOOP_HOME/bin/hadoop fs -put $HADOOP_HOME/conf input
$HADOOP_HOME/bin/hadoop jar $HADOOP_HOME/hadoop-examples-1.2.1.jar grep input output 'dfs[a-z.]+'
$HADOOP_HOME/bin/hadoop fs -get output output

### ...your job goes here...

# Stop HADOOP services
$MYHADOOP_HOME/bin/myhadoop-shutdown.sh
