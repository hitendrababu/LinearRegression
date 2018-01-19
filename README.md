# LinearRegression
VENKAT HITENDRA NARLA
800936744
email - vnarla@uncc.edu

INSTRUCTIONS
1. First create a directory for input in hdfs
   The commands are:
   sudo su hdfs
   hadoop fs -mkdir /user/cloudera
   hadoop fs -mkdir /user/cloudera/linreg /user/cloudera/linreg/input
   hadoop fs -put <input path for the files in your directory> /user/cloudera/linreg/input
2. For running the file in spark, the command is:
   spark - submit linreg.py /user/cloudera/linreg/input/yxlin.csv
3. Copy the output and paste it in text file
   
