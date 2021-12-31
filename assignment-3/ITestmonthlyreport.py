import unittest
import helper
from pyspark import SparkContext
import os
os.environ['JAVA_HOME']='/usr/lib/jvm/java-11-openjdk-amd64'
import datetime
import sys
import helper
from pyspark.sql.functions import instr
from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType, StringType,\
IntegerType, StructType, StructField
from pyspark.sql.functions import *
import calendar

class ITestmonthlyreport(unittest.TestCase):

    def test_total_monthly_report_of_all_within_may(self):
        """Positive Test-Case that checks total sale made by All items within May month."""
        print("*********************************************************************")
        print("testing****")
        spark = SparkSession.builder.getOrCreate()
        schema = StructType([StructField("Time", TimestampType()), \
                             StructField("Item_Id", StringType()), \
                             StructField("Price", IntegerType())])
        sales = spark.read.csv('sales_data/sales.csv', header=False, \
                               timestampFormat='yyyy-MM-dd HH:mm', schema=schema)
        subclothes = helper.getSubCategoryOfCategory("Clothes", "sales_data/categories.csv")
        Subsubclothes = helper.getItemIdsFromSubCategory(subclothes, "sales_data/item_categories.csv")
        df = sales.where(~sales.Item_Id.isin(Subsubclothes))
        filterdataframe = df.select(col("Time"),
                                    month(col("Time")).alias("month"), col("Item_Id"), col("Price")
                                    ).groupBy("month", "Item_Id").sum("Price").sort(desc("Item_Id")).withColumnRenamed(
            "sum(Price)", "Sale").collect()
        monthlyItemSales = helper.convertIdtoItemName(filterdataframe, "sales_data/item_categories.csv")
        CategoryMapper = helper.LoadCategoryMapper("sales_data/categories.csv")
        monthlySubCategorySales = helper.monthlysalesofSubcategory(monthlyItemSales, CategoryMapper)
        monthlyCategorySales = helper.monthlysalesOfEachCategory(monthlySubCategorySales, CategoryMapper)
        print(monthlyItemSales)
        print("**************************")
        print(monthlySubCategorySales)
        print("**************************")
        print(monthlyCategorySales)
        self.assertEqual(4411680, monthlyCategorySales[('All','May')])
