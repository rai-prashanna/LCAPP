import unittest
import helper
import os
os.environ['JAVA_HOME']='/usr/lib/jvm/java-11-openjdk-amd64'

from pyspark import SparkContext
import helper

class BlackBoxTesting():

    def test_assignment1(self):
        """Positive Test-Case ."""
        sc = SparkContext("local", "Simple App")
        wordcount(sc, "test_data/sales.csv", "test.assingment1.out")
        ItemCategoryMapper = helper.LoadItemIdItemName()
        SalesbyName = helper.MapItemIDtoName(ItemCategoryMapper)
        CategoryMapper = helper.LoadCategoryMapper()
        salesofEachSubCategory = helper.SalesofEachSubCategory(SalesbyName, CategoryMapper)
        salesOfEachCategory = helper.salesOfEachCategory(salesofEachSubCategory, CategoryMapper)
        self.assertEqual(SalesbyName, SalesbyName)

