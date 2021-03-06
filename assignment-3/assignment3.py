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
from pyspark.sql.functions import *
import calendar

#my  code
subclothes=helper.getSubCategoryOfCategory("Clothes","sales_data/categories.csv")
Subsubclothes=helper.getItemIdsFromSubCategory(subclothes,"sales_data/item_categories.csv")
df=sales.where(~sales.Item_Id.isin(Subsubclothes))
filterdataframe=df.select(col("Time"),
     month(col("Time")).alias("month"),col("Item_Id"),col("Price")
  ).groupBy("month","Item_Id").sum("Price").sort(desc("Item_Id")).withColumnRenamed("sum(Price)","Sale").collect()
monthlyItemSales=helper.convertIdtoItemName(filterdataframe,"sales_data/item_categories.csv")
CategoryMapper = helper.LoadCategoryMapper("sales_data/categories.csv")
monthlySubCategorySales=helper.monthlysalesofSubcategory(monthlyItemSales,CategoryMapper)
monthlyCategorySales=helper.monthlysalesOfEachCategory(monthlySubCategorySales,CategoryMapper)
print(monthlyItemSales)
print("**************************")
print(monthlySubCategorySales)
print("**************************")
print(monthlyCategorySales)
