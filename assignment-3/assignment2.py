# next two lines are required if your default Java is > 1.8
# the precise location depends on your machine
import os
import unittest

os.environ['JAVA_HOME']='/usr/lib/jvm/java-11-openjdk-amd64'
import datetime
import sys
from operator import add
from pyspark import SparkContext
import helper


sc = SparkContext("local", "Simple App")
input = sc.textFile("sales_data/sales.csv")
rdd = input.map(lambda line: (helper.lineSplitterByWeek(line)))
rawSalesrecord=rdd.reduceByKey(lambda x,y : int(x)+int(y)).sortByKey().collect()
CategoryMapper = helper.LoadCategoryMapper("sales_data/categories.csv")
salesByname=helper.weeklyconvertIdtoItemName(rawSalesrecord,"sales_data/item_categories.csv")
print(salesByname)
weeklysalesOFSubCategory=helper.weeklySalesSubCategory(salesByname,CategoryMapper)
print(weeklysalesOFSubCategory)
weeklySalesofCategory=helper.weeklysalesOfEachCategory(weeklysalesOFSubCategory,CategoryMapper)
print(weeklySalesofCategory)


print("*********************************************************************")
print("testing****")
input = sc.textFile("test_data/sales.csv")
rdd = input.map(lambda line: (helper.lineSplitterByWeek(line)))
rawSalesrecord=rdd.reduceByKey(lambda x,y : int(x)+int(y)).sortByKey().collect()
CategoryMapper = helper.LoadCategoryMapper("test_data/categories.csv")
salesByname=helper.weeklyconvertIdtoItemName(rawSalesrecord,"test_data/item_categories.csv")
print(salesByname)
weeklysalesOFSubCategory=helper.weeklySalesSubCategory(salesByname,CategoryMapper)
print(weeklysalesOFSubCategory)
weeklySalesofCategory=helper.weeklysalesOfEachCategory(weeklysalesOFSubCategory,CategoryMapper)
print(weeklySalesofCategory)

class ITestweekelyreport(unittest.TestCase):

    def test_total_weekly_report_of_all(self):
        """Positive Test-Case that checks total sale made by All within weekend."""
        self.assertEqual(4800, weeklySalesofCategory[('All', 'weekend')])




