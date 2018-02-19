#!/bin/bash

HomeDir=/user/cloudera/average
Hdfs_Input=/user/cloudera/average/input
Hdfs_Output=/user/cloudera/average/output

echo "Copy input file" 
Input_File=input.txt
echo "Copy input file to HDFS"
hadoop fs -mkdir $HomeDir
hadoop fs -mkdir $Hdfs_Input
hadoop fs -put $Input_File $Hdfs_Input

echo "Run Average In-Mapper algorithm"
hadoop jar AverageComputationInMapper.jar $Hdfs_Input ${Hdfs_Output}