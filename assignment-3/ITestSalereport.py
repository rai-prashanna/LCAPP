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
import assignment1
class ITestsaleReport(unittest.TestCase):

    def test_total_sale_report_of_Clothes(self):
        """Positive Test-Case that checks total sale made by Clothes."""
        print("*********************************************************************")
        print("*********************************************************************")
        print("testing****")
        sc = SparkContext("local", "Simple App")
        assignment1.wordcount(sc, "test_data/sales.csv", "test.assingment1.out")
        ItemCategoryMapper = helper.LoadItemIdItemName("test_data/item_categories.csv")
        SalesbyName = helper.MapItemIDtoName(ItemCategoryMapper, "test.assingment1.out/part-00000")
        CategoryMapper = helper.LoadCategoryMapper("test_data/categories.csv")
        salesofEachSubCategory = helper.SalesofEachSubCategory(SalesbyName, CategoryMapper)
        salesOfEachCategory = helper.salesOfEachCategory(salesofEachSubCategory, CategoryMapper)
        print(SalesbyName)
        print("***************************************************")
        print(salesofEachSubCategory)
        print("***************************************************")
        print(salesOfEachCategory)
        self.assertEqual(4800, salesOfEachCategory[('Clothes')])
