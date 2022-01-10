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
        weeklySalesofCategory = helper.weeklysalesOfEachCategory(weeklysalesOFSubCategory, CategoryMapper)
        print("***************************************************")
        print("Electrical,weekday:"+str(weeklysalesOFSubCategory[('Electrical','weekday')]))
        print("***************************************************")
        print("Clothes,'weekday':"+str(weeklySalesofCategory[('Clothes','weekday')]))
        print("Books,'weekday':"+str(weeklySalesofCategory[('Books','weekday')]))
        print("White Goods,'weekday':"+str(weeklySalesofCategory[('White Goods','weekday')]))
        print("Clothes,'weekday':"+str(weeklySalesofCategory[('Clothes','weekday')]))
        print("Mens Clothes,'weekday':"+str(weeklysalesOFSubCategory[('Mens Clothes','weekday')]))
        print("Womens Clothes,'weekday':"+str(weeklysalesOFSubCategory[('Womens Clothes','weekday')]))
        print("All ,'weekday':"+str(weeklySalesofCategory[('All','weekday')]))

        self.assertEqual(8000, weeklySalesofCategory[('Clothes','weekday')])
        self.assertEqual(22400, weeklySalesofCategory[('All','weekday')])
        self.assertEqual(6400, weeklySalesofCategory[('Books','weekday')])
        self.assertEqual(4000, weeklysalesOFSubCategory[('Electrical','weekday')])
        self.assertEqual(4000, weeklySalesofCategory[('White Goods','weekday')])


