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
        sales = spark.read.csv('test_data/sales.csv', header=False, \
                               timestampFormat='yyyy-MM-dd HH:mm', schema=schema)
        subclothes = helper.getSubCategoryOfCategory("Clothes", "test_data/categories.csv")
        Subsubclothes = helper.getItemIdsFromSubCategory(subclothes, "test_data/item_categories.csv")
        df = sales.where(~sales.Item_Id.isin(Subsubclothes))
        filterdataframe = df.select(col("Time"),
                                    month(col("Time")).alias("month"), col("Item_Id"), col("Price")
                                    ).groupBy("month", "Item_Id").sum("Price").sort(desc("Item_Id")).withColumnRenamed(
            "sum(Price)", "Sale").collect()
        monthlyItemSales = helper.convertIdtoItemName(filterdataframe, "test_data/item_categories.csv")
        print(monthlyItemSales)
        CategoryMapper = helper.LoadCategoryMapper("test_data/categories.csv")
        monthlySubCategorySales = helper.monthlysalesofSubcategory(monthlyItemSales, CategoryMapper)
        monthlyCategorySales = helper.monthlysalesOfEachCategory(monthlySubCategorySales, CategoryMapper)
        print(monthlyItemSales)
        print("**************************")
        print("***************************************************")
        print("Electrical,weekday:"+str(monthlySubCategorySales[('Electrical','May')]))
        print("***************************************************")
        print("Books,'weekday':"+str(monthlyCategorySales[('Books','May')]))
        print("White Goods,'weekday':"+str(monthlyCategorySales[('White Goods','May')]))
        print("All ,'weekday':"+str(monthlyCategorySales[('All','May')]))

        self.assertEqual(1800, monthlyCategorySales[('All','May')])
        self.assertEqual(800, monthlyCategorySales[('Books','May')])
        self.assertEqual(500, monthlySubCategorySales[('Electrical','May')])
        self.assertEqual(500, monthlyCategorySales[('White Goods','May')])
