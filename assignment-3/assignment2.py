# next two lines are required if your default Java is > 1.8
# the precise location depends on your machine
import os
os.environ['JAVA_HOME']='/usr/lib/jvm/java-11-openjdk-amd64'
import datetime
import sys
from operator import add
from pyspark import SparkContext
import helper


sc = SparkContext("local", "Simple App")
input = sc.textFile("sales_data/sales.csv")
rdd = input.map(lambda line: (helper.lineSplitter(line)))
#output=rdd.reduceByKey(lambda x,y : int(x)+int(y)).groupByKey().map(lambda x : (x[0],list(x[1]))).collect()
output=rdd.reduceByKey(lambda x,y : int(x)+int(y)).sortByKey().collect()

print(output)


