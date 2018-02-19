#!/bin/bash

HomeDir=/user/cloudera/relativefrequency
Hdfs_Input=/user/cloudera/relativefrequency/input
Hdfs_Output=/user/cloudera/relativefrequency/output


echo "Create input file" 
Input_File=input.txt
if [ ! -f "$Input_File" ]; then
	echo "B12 C31 D76 A12 B76 B12 D76 C31 A10 B12 D76" > $Input_File
	echo >> $Input_File
	echo "C31 D76 B12 A12 C31 D76 B12  A12 D76 A12 D76" >> $Input_File
fi

echo "Copy input file to HDFS"
hadoop fs -mkdir $HomeDir
hadoop fs -mkdir $Hdfs_Input
hadoop fs -put $Input_File $Hdfs_Input

echo "Run Pair In-Mapper algorithm"
hadoop jar RelativeFrequencyPairInMapper.jar $Hdfs_Input ${Hdfs_Output}_pair_in_mapper