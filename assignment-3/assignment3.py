# next two lines are required if your default Java is > 1.8
# the precise location depends on your machine
import os
os.environ['JAVA_HOME']='/usr/lib/jvm/java-11-openjdk-amd64'
import datetime
import sys
import helper
from pyspark.sql.functions import instr
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType, StringType,\
IntegerType, StructType, StructField
spark = SparkSession.builder.getOrCreate()
schema = StructType([StructField("Time", TimestampType()),\
StructField("Item_Id", StringType()),\
StructField("Price", IntegerType())])
sales = spark.read.csv('sales_data/sales.csv', header = False,\
timestampFormat='yyyy-MM-dd HH:mm', schema=schema)


#my  code
subclothes=helper.getSubCategoryOfCategory("Clothes")
Subsubclothes=helper.getItemIdsFromSubCategory(subclothes)
sales.where(~sales.Item_Id.isin(Subsubclothes)).show(23000, False)
    #write.format("csv").mode("overwrite").save("myfile.csv")

