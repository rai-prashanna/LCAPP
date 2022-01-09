import unittest
import helper
from pyspark import SparkContext

class ITestweekelyreport(unittest.TestCase):

    def test_total_weekly_report_of_all(self):
        """Positive Test-Case that checks total sale made by All within weekend."""
        print("*********************************************************************")
        print("testing****")
        sc = SparkContext("local", "Simple App")
        input = sc.textFile("test_data/sales.csv")
        rdd = input.map(lambda line: (helper.lineSplitterByWeek(line)))
        rawSalesrecord = rdd.reduceByKey(lambda x, y: int(x) + int(y)).sortByKey().collect()
        CategoryMapper = helper.LoadCategoryMapper("test_data/categories.csv")
        salesByname = helper.weeklyconvertIdtoItemName(rawSalesrecord, "test_data/item_categories.csv")
        print(salesByname)
        weeklysalesOFSubCategory = helper.weeklySalesSubCategory(salesByname, CategoryMapper)
        print(weeklysalesOFSubCategory)
        weeklySalesofCategory = helper.weeklysalesOfEachCategory(weeklysalesOFSubCategory, CategoryMapper)
        print(weeklySalesofCategory)
        self.assertEqual(12400, weeklySalesofCategory[('All', 'weekend')])
        self.assertEqual(3200, weeklySalesofCategory[('Books', 'weekend')])
        self.assertEqual(8000, weeklySalesofCategory[('Clothes', 'weekday')])


